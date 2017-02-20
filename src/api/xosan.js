import fromPairs from 'lodash/fromPairs'
import fs from 'fs-promise'
import map from 'lodash/map'
import arp from 'arp-a'
import { spawn } from 'child_process'

const SSH_KEY_FILE = 'id_rsa_xosan'
const NETWORK_PREFIX = '172.31.100.'

const XOSAN_VM_SYSTEM_DISK_SIZE = 10 * 1024 * 1024 * 1024
const XOSAN_DATA_DISK_USEAGE_RATIO = 0.99

async function runCmd (command, argArray) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, argArray)
    let resp = ''
    let stderr = ''
    child.stdout.on('data', function (buffer) {
      resp += buffer.toString()
    })
    child.stderr.on('data', function (buffer) {
      stderr += buffer.toString()
    })
    child.on('close', (code) => {
      if (code !== 0) {
        reject(stderr)
      } else {
        resolve(resp)
      }
    })
  })
}

export async function getVolumeInfo ({ sr }) {
  const xapi = this.getXapi(sr)
  const giantIPtoVMDict = {}
  const nodes = xapi.xo.getData(sr, 'xosan_config').nodes
  if (!nodes) {
    return null;
  }
  nodes.forEach(conf => {
    giantIPtoVMDict[conf.vm.ip] = xapi.getObject(conf.vm.id)
  })
  const oneHostAndVm = nodes[0]
  const resultCmd = await remoteSsh(xapi, {
    host: xapi.getObject(oneHostAndVm.host),
    address: oneHostAndVm.vm.ip
  }, 'gluster volume info xosan')
  const result = resultCmd['stdout']

  /*
   Volume Name: xosan
   Type: Disperse
   Volume ID: 1d4d0e57-8b6b-43f9-9d40-c48be1df7548
   Status: Started
   Snapshot Count: 0
   Number of Bricks: 1 x (2 + 1) = 3
   Transport-type: tcp
   Bricks:
   Brick1: 192.168.0.201:/bricks/brick1/xosan1
   Brick2: 192.168.0.202:/bricks/brick1/xosan1
   Brick3: 192.168.0.203:/bricks/brick1/xosan1
   Options Reconfigured:
   client.event-threads: 16
   server.event-threads: 16
   performance.client-io-threads: on
   nfs.disable: on
   performance.readdir-ahead: on
   transport.address-family: inet
   features.shard: on
   features.shard-block-size: 64MB
   network.remote-dio: enable
   cluster.eager-lock: enable
   performance.io-cache: off
   performance.read-ahead: off
   performance.quick-read: off
   performance.stat-prefetch: on
   performance.strict-write-ordering: off
   cluster.server-quorum-type: server
   cluster.quorum-type: auto
   */
  let info = fromPairs(result.trim().split('\n')
    .map(line => line.split(':').map(val => val.trim()))
    // some lines have more than one ':', re-assemble them
    .map(line => [line[0], line.slice(1).join(':')]))
  let getNumber = item => parseInt(item.substr(5))
  let brickKeys = Object.keys(info).filter(key => key.match(/^Brick[1-9]/)).sort((i1, i2) => getNumber(i1) - getNumber(i2))
  // expected brickKeys : [ 'Brick1', 'Brick2', 'Brick3' ]
  info['Bricks'] = brickKeys.map(key => {
    const ip = info[key].split(':')[0]
    return { config: info[key], ip: ip, vm: giantIPtoVMDict[ip] }
  })
  await new Promise((resolve, reject) => arp.table((err, entry) => {
    if (entry) {
      const brick = info['Bricks'].find(element => element.config.split(':')[0] === entry.ip)
      if (brick) {
        brick.mac = entry.mac
      }
    }
    if (!entry && !err) {
      resolve()
    }
  }))
  return info
}

getVolumeInfo.description = 'info on gluster volume'
getVolumeInfo.permission = 'admin'

getVolumeInfo.params = {
  sr: {
    type: 'string'
  }
}
getVolumeInfo.resolve = {
  sr: ['sr', 'SR', 'administrate']
}
function trucate2048 (value) {
  return 2048 * Math.floor(value / 2048)
}

async function copyVm (xapi, originalVm, params) {
  return { vm: await xapi.copyVm(originalVm, params.sr), params }
}

async function prepareGlusterVm (xapi, vmAndParam, xosanNetwork) {
  let vm = vmAndParam.vm
  // refresh the object so that sizes are correct
  const params = vmAndParam.params
  const ip = params.xenstore_data['vm-data/ip']
  const sr = xapi.getObject(params.sr.$id)
  await xapi._waitObjectState(sr.$id, sr => Boolean(sr.$PBDs))
  let host = xapi.getObject(xapi.getObject(sr.$PBDs[0]).host)
  let firstVif = vm.$VIFs[0]
  if (xosanNetwork.$id !== firstVif.$network.$id) {
    console.log('VIF in wrong network (' + firstVif.$network.name_label + '), moving to correct one: ' + xosanNetwork.name_label)
    await xapi.call('VIF.move', firstVif.$ref, xosanNetwork.$ref)
  }
  await xapi.editVm(vm, {
    name_label: params.name_label,
    name_description: params.name_description
  })
  await xapi.call('VM.set_xenstore_data', vm.$ref, params.xenstore_data)
  const dataDisk = vm.$VBDs.map(vbd => vbd.$VDI).find(vdi => vdi && vdi.name_label === 'xosan_data')
  const srFreeSpace = sr.physical_size - sr.physical_utilisation
  // we use a percentage because it looks like the VDI overhead is proportional
  const newSize = trucate2048((srFreeSpace + dataDisk.virtual_size) * XOSAN_DATA_DISK_USEAGE_RATIO)
  await xapi._resizeVdi(dataDisk, newSize)
  await xapi.startVm(vm)
  console.log('waiting for boot of ', ip)
  // wait until we find the assigned IP in the networks, we are just checking the boot is complete
  const vmIsUp = vm => Boolean(vm.$guest_metrics && Object.values(vm.$guest_metrics.networks).find(value => value === ip))
  vm = await xapi._waitObjectState(vm.$id, vmIsUp)
  console.log('booted ', ip)
  return { address: ip, host, vm }
}

async function callPlugin (xapi, host, command, params) {
  console.log('calling plugin', host.address, command)
  return JSON.parse(await xapi.call('host.call_plugin', host.$ref, 'xosan.py', command, params))
}

async function remoteSsh (xapi, hostAndAddress, cmd) {
  const result = await callPlugin(xapi, hostAndAddress.host, 'run_ssh', {
    destination: 'root@' + hostAndAddress.address,
    cmd: cmd
  })
  if (result.exit !== 0) {
    throw new Error('ssh error: ' + JSON.stringify(result))
  }
  return result
}

async function setPifIp (xapi, pif, address) {
  await xapi.call('PIF.reconfigure_ip', pif.$ref, 'Static', address, '255.255.255.0', NETWORK_PREFIX + '1', '')
}

export async function createSR ({ template, pif, vlan, srs, glusterType, redundancy }) {
  if (!this.requestResource) {
    throw new Error('requestResource is not a function')
  }

  if (srs.length < 1) {
    return // TODO: throw an error
  }

  let vmIpLastNumber = 101
  let hostIpLastNumber = 1
  let xapi = this.getXapi(srs[0])
  let xosanNetwork = await xapi.createNetwork({
    name: 'XOSAN network',
    description: 'XOSAN network',
    pifId: pif._xapiId,
    mtu: 9000,
    vlan: +vlan
  })
  await Promise.all(xosanNetwork.$PIFs.map(pif => setPifIp(xapi, pif, NETWORK_PREFIX + (hostIpLastNumber++))))
  let sshKey = xapi.xo.getData(xapi.pool, 'xosan_ssh_key')
  if (!sshKey) {
    try {
      await fs.access(SSH_KEY_FILE, fs.constants.R_OK)
    } catch (e) {
      await runCmd('ssh-keygen', ['-q', '-f', SSH_KEY_FILE, '-t', 'rsa', '-b', '4096', '-N', ''])
    }
    sshKey = {
      private: await fs.readFile(SSH_KEY_FILE, 'ascii'),
      public: await fs.readFile(SSH_KEY_FILE + '.pub', 'ascii')
    }
    xapi.xo.setData(xapi.pool, 'xosan_ssh_key', sshKey)
  }
  const publicKey = sshKey.public
  const privateKey = sshKey.private
  const srsObjects = map(srs, srId => xapi.getObject(srId))

  const vmParameters = map(srs, srId => {
    const sr = xapi.getObject(srId)
    const host = xapi.getObject(xapi.getObject(sr.$PBDs[0]).host)
    return {
      sr,
      host,
      name_label: 'XOSAN - ' + sr.name_label + ' - ' + host.name_label,
      name_description: 'Xosan VM storing data on volume ' + sr.name_label,
      // the values of the xenstore_data object *have* to be string, don't forget.
      xenstore_data: {
        'vm-data/hostname': 'XOSAN' + sr.name_label,
        'vm-data/sshkey': publicKey,
        'vm-data/ip': NETWORK_PREFIX + (vmIpLastNumber++),
        'vm-data/mtu': String(xosanNetwork.MTU),
        'vm-data/vlan': String(vlan)
      }
    }
  })
  await Promise.all(vmParameters.map(vmParam => callPlugin(xapi, vmParam.host, 'receive_ssh_keys', {
    private_key: privateKey,
    public_key: publicKey,
    force: 'true'
  })))
  const request = await this.requestResource('xosan', template.id, template.version)
  request.on('response', async response => {
    const firstVM = await xapi.importVm(response, { srId: vmParameters[0].sr.$ref, type: 'xva' })
    await xapi.editVm(firstVM, {
      autoPoweron: true
    })
    const vmsAndParams = [{
      vm: firstVM,
      params: vmParameters[0]
    }].concat(await Promise.all(vmParameters.slice(1).map(param => copyVm(xapi, firstVM, param))))

    const ipAndHosts = await Promise.all(map(vmsAndParams, vmAndParam => prepareGlusterVm(xapi, vmAndParam, xosanNetwork)))
    const firstIpAndHost = ipAndHosts[0]
    for (let i = 1; i < ipAndHosts.length; i++) {
      console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster peer probe ' + ipAndHosts[i].address))
    }
    const configByType = {
      replica: {
        creation: 'replica ' + redundancy + ' ',
        extra: ['gluster volume set xosan cluster.data-self-heal on']
      },
      disperse: {
        creation: 'disperse ' + ipAndHosts.length + ' redundancy ' + redundancy + ' ',
        extra: []
      }
    }

    const volumeCreation = 'gluster volume create xosan ' + configByType[glusterType].creation +
      ' ' + ipAndHosts.map(ipAndHosts => (ipAndHosts.address + ':/bricks/xosan/xosandir')).join(' ')
    console.log('creating volume: ', volumeCreation)
    console.log(await remoteSsh(xapi, firstIpAndHost, volumeCreation))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan network.remote-dio enable'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan cluster.eager-lock enable'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan performance.io-cache off'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan performance.read-ahead off'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan performance.quick-read off'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan performance.strict-write-ordering off'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan client.event-threads 8'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan server.event-threads 8'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan performance.io-thread-count 64'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan performance.stat-prefetch on'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan features.shard on'))
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan features.shard-block-size 512MB'))
    for (let confChunk of configByType[glusterType].extra) {
      console.log(await remoteSsh(xapi, firstIpAndHost, confChunk))
    }
    console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume start xosan'))

    console.log('xosan gluster volume started')
    const config = { server: firstIpAndHost.address + ':/xosan' }
    const xosanSr = await xapi.call('SR.create', srsObjects[0].$PBDs[0].$host.$ref, config, 0, 'XOSAN', 'XOSAN', 'xosan', '', true, {})
    await xapi.xo.setData(xosanSr, 'xosan_config', {
      nodes: ipAndHosts.map(param => ({
        host: param.host.$id,
        vm: { id: param.vm.$id, ip: param.address }
      })),
      network: xosanNetwork.$id
    })
  })
}

createSR.description = 'create gluster VM'
createSR.permission = 'admin'
createSR.params = {
  srs: {
    type: 'array',
    items: {
      type: 'string'
    }
  },
  pif: {
    type: 'string'
  },
  vlan: {
    type: 'string'
  },
  glusterType: {
    type: 'string'
  },
  redundancy: {
    type: 'number'
  }
}

createSR.resolve = {
  srs: ['sr', 'SR', 'administrate'],
  pif: ['pif', 'PIF', 'administrate']
}

const POSSIBLE_CONFIGURATIONS = []
POSSIBLE_CONFIGURATIONS[2] = [{ layout: 'replica', redundancy: 2, capacity: 1 }]
POSSIBLE_CONFIGURATIONS[3] = [
  { layout: 'disperse', redundancy: 1, capacity: 2 },
  { layout: 'replica', redundancy: 3, capacity: 1 }]
POSSIBLE_CONFIGURATIONS[4] = [{ layout: 'replica', redundancy: 2, capacity: 1 }]
POSSIBLE_CONFIGURATIONS[5] = [{ layout: 'disperse', redundancy: 1, capacity: 4 }]
POSSIBLE_CONFIGURATIONS[6] = [
  { layout: 'disperse', redundancy: 2, capacity: 4 },
  { layout: 'replica', redundancy: 2, capacity: 3 },
  { layout: 'replica', redundancy: 3, capacity: 2 }]
POSSIBLE_CONFIGURATIONS[7] = [{ layout: 'disperse', redundancy: 3, capacity: 4 }]
POSSIBLE_CONFIGURATIONS[8] = [{ layout: 'replica', redundancy: 2, capacity: 4 }]
POSSIBLE_CONFIGURATIONS[9] = [
  { layout: 'disperse', redundancy: 1, capacity: 8 },
  { layout: 'replica', redundancy: 3, capacity: 3 }]
POSSIBLE_CONFIGURATIONS[10] = [
  { layout: 'disperse', redundancy: 2, capacity: 8 },
  { layout: 'replica', redundancy: 2, capacity: 5 }]
POSSIBLE_CONFIGURATIONS[11] = [{ layout: 'disperse', redundancy: 3, capacity: 8 }]
POSSIBLE_CONFIGURATIONS[12] = [
  { layout: 'disperse', redundancy: 4, capacity: 8 },
  { layout: 'replica', redundancy: 2, capacity: 6 }]
POSSIBLE_CONFIGURATIONS[13] = [{ layout: 'disperse', redundancy: 5, capacity: 8 }]
POSSIBLE_CONFIGURATIONS[14] = [
  { layout: 'disperse', redundancy: 6, capacity: 8 },
  { layout: 'replica', redundancy: 2, capacity: 7 }]
POSSIBLE_CONFIGURATIONS[15] = [
  { layout: 'disperse', redundancy: 7, capacity: 8 },
  { layout: 'replica', redundancy: 3, capacity: 5 }]
POSSIBLE_CONFIGURATIONS[16] = [{ layout: 'replica', redundancy: 2, capacity: 8 }]

export async function computeXosanPossibleOptions ({ lvmSrs }) {
  const count = lvmSrs.length
  const configurations = POSSIBLE_CONFIGURATIONS[count]
  if (!configurations) {
    return null
  }
  if (count > 0) {
    let xapi = this.getXapi(lvmSrs[0])
    let srs = map(lvmSrs, srId => xapi.getObject(srId))
    let srSizes = map(srs, sr => sr.physical_size - sr.physical_utilisation)
    let minSize = Math.min.apply(null, srSizes)
    let brickSize = (minSize - XOSAN_VM_SYSTEM_DISK_SIZE) * XOSAN_DATA_DISK_USEAGE_RATIO
    return configurations.map(conf => Object.assign({}, conf, { availableSpace: brickSize * conf.capacity }))
  }
}

computeXosanPossibleOptions.params = {
  lvmSrs: {
    type: 'array',
    items: {
      type: 'string'
    }
  }
}

// ---------------------------------------------------------------------

export async function downloadAndInstallXosanPack ({ namespace, id, version, pool }) {
  if (!this.requestResource) {
    throw new Error('requestResource is not a function')
  }

  const req = await this.requestResource(namespace, id, version)
  console.log('pool.id', pool.id)
  const xapi = this.getXapi(pool.id)
  req.on('response', res => {
    res.length = res.headers['content-length']
    return xapi.installSupplementalPackOnAllHosts(res)
  })

  return
}

downloadAndInstallXosanPack.description = 'Register a resource via cloud plugin'

downloadAndInstallXosanPack.params = {
  namespace: { type: 'string' },
  id: { type: 'string' },
  version: { type: 'string' },
  pool: { type: 'string' }
}

downloadAndInstallXosanPack.resolve = {
  pool: ['pool', 'pool', 'administrate']
}

downloadAndInstallXosanPack.permission = 'admin'
