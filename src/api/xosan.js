import fromPairs from 'lodash/fromPairs'
import find from 'lodash/find'
import fs from 'fs-promise'
import map from 'lodash/map'
import arp from 'arp-a'
import { spawn } from 'child_process'
import { createReadStream } from 'fs'


const SSH_KEY_FILE = 'id_rsa_xosan'
const NETWORK_PREFIX = '172.31.100.'

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

async function runSsh (ip, command) {
  return await runCmd('ssh', [ '-o', 'StrictHostKeyChecking=no', 'root@' + ip, command ])
}

export async function getPeers ({ ip }) {
  // ssh -o StrictHostKeyChecking=no  root@192.168.0.201 gluster pool list
  const result = await runSsh(ip, 'gluster pool list')
  /* expected result:
   UUID\t\t\t\t\tHostname     \tState
   953f8259-5ddf-4459-9846-933433cc7787\t192.168.0.202\t  Connected
   b4a98ab8-4634-4916-9be6-c980298fe5ed\t192.168.0.203\tConnected
   1ec28018-92ea-4662-b3da-fcb11c128c07\tlocalhost    \tConnected
   * */
  let peers = result.trim().split('\n').slice(1)
    .map(line => (line.split('\t').map(elem => elem.trim())))
    .map(line => line[ 1 ] === 'localhost' ? [ line[ 0 ], ip, line[ 2 ] ] : line)
    .map(line => ({ uuid: line[ 0 ], hostname: line[ 1 ], state: line[ 2 ] }))
    .sort((l1, l2) => l1.hostname.localeCompare(l2.hostname))
  /* and now:
   [
   { uuid: '1ec28018-92ea-4662-b3da-fcb11c128c07', hostname: '192.168.0.201', state: 'Connected' },
   { uuid: '953f8259-5ddf-4459-9846-933433cc7787', hostname: '192.168.0.202', state: 'Connected' },
   { uuid: 'b4a98ab8-4634-4916-9be6-c980298fe5ed', hostname: '192.168.0.203', state: 'Connected' }
   ]
   */
  await new Promise((resolve, reject) => arp.table((err, entry) => {
    if (entry) {
      const peer = peers.find(element => element.hostname === entry.ip)
      if (peer) {
        peer.mac = entry.mac
      }
    }
    if (!entry && !err) {
      resolve(peers)
    }
  }))
  return peers
}

getPeers.description = 'find a gluster server peers'

getPeers.permission = 'admin'

getPeers.params = {
  ip: {
    type: 'string'
  }
}

export async function getVolumeInfo ({ sr }) {
  const xapi = this.getXapi(sr)
  const config = xapi.xo.getData(sr, 'xosan_config')
  const giantIPtoVMDict = {}
  config.forEach(conf => {
    giantIPtoVMDict[ conf.vm.ip ] = xapi.getObject(conf.vm.id)
  })
  const oneHostAndVm = config[ 0 ]
  const resultCmd = await remoteSsh(xapi, {
    host: xapi.getObject(oneHostAndVm.host),
    address: oneHostAndVm.vm.ip
  }, 'gluster volume info xosan')
  const result = resultCmd[ 'stdout' ]

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
    // some lines have more than one ":", re-assemble them
    .map(line => [ line[ 0 ], line.slice(1).join(':') ]))
  let getNumber = item => parseInt(item.substr(5))
  let brickKeys = Object.keys(info).filter(key => key.match(/^Brick[1-9]/)).sort((i1, i2) => getNumber(i1) - getNumber(i2))
  // expected brickKeys : [ 'Brick1', 'Brick2', 'Brick3' ]
  info[ 'Bricks' ] = brickKeys.map(key => {
    const ip = info[ key ].split(':')[ 0 ]
    return { config: info[ key ], ip: ip, vm: giantIPtoVMDict[ ip ] }
  })
  await new Promise((resolve, reject) => arp.table((err, entry) => {
    if (entry) {
      const brick = info[ 'Bricks' ].find(element => element.config.split(':')[ 0 ] === entry.ip)
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
  sr: [ 'sr', 'SR', 'administrate' ]
}
function trucate2048 (value) {
  return 2048 * Math.floor(value / 2048)
}

async function importVM (xapi, sr) {
  return await xapi.importVm(createReadStream('../XOSANTEMPLATE.xva'), { srId: sr.$ref, type: 'xva' })
}

async function copyVm (xapi, originalVm, params) {
  return { vm: await xapi.copyVm(originalVm, params.sr), params }
}

async function prepareGlusterVm (xapi, vmAndParam, xosanNetwork) {
  let vm = vmAndParam.vm
  //refresh the object so that sizes are correct
  const params = vmAndParam.params
  const ip = params.xenstore_data[ 'vm-data/ip' ]
  const sr = xapi.getObject(params.sr.$id)
  await xapi._waitObjectState(sr.$id, sr => Boolean(sr.$PBDs))
  let host = xapi.getObject(xapi.getObject(sr.$PBDs[ 0 ]).host)
  let firstVif = vm.$VIFs[ 0 ]
  if (xosanNetwork.$id !== firstVif.$network.$id) {
    console.log('VIF in wrong network (' + firstVif.$network.name_label + '), moving to correct one: ' + xosanNetwork.name_label)
    await xapi.call('VIF.move', firstVif.$ref, xosanNetwork.$ref)
  }
  await xapi.editVm(vm, {
    name_label: params.name_label,
    name_description: params.name_description
  })
  await xapi.call('VM.set_xenstore_data', vm.$ref, params.xenstore_data)
  const dataDisk = vm.$VBDs.map(vbd => vbd.$VDI).find(vdi => vdi && vdi.name_label === 'data')
  const srFreeSpace = sr.physical_size - sr.physical_utilisation
  //we use a percentage because it looks like the VDI overhead is proportional
  const newSize = trucate2048((srFreeSpace + dataDisk.virtual_size) * 0.99)
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
  return await callPlugin(xapi, hostAndAddress.host, 'run_ssh', {
    destination: 'root@' + hostAndAddress.address,
    cmd: cmd
  })
}

async function setPifIp (xapi, pif, address) {
  console.log('before PIF.reconfigure_ip', address)
  await xapi.call('PIF.reconfigure_ip', pif.$ref, 'Static', address, '255.255.255.0', NETWORK_PREFIX + '1', '')
  console.log('after PIF.reconfigure_ip')
}

export async function createVM ({ pif, vlan, srs }) {
  console.log('vlan', vlan)
  console.log('pif', pif)
  let vmIpLastNumber = 101
  let hostIpLastNumber = 1
  try {
    if (srs.length > 0) {
      let xapi = find(this.getAllXapis(), xapi => (xapi.getObject(srs[ 0 ])))
      let xosanNetwork = await xapi.createNetwork({
        name: 'XOSAN network',
        description: 'XOSAN network',
        pifId: pif._xapiId,
        mtu: 9000,
        vlan: +vlan
      })
      console.log('network created')
      console.log('newNetwork', xosanNetwork)
      await Promise.all(xosanNetwork.$PIFs.map(pif => setPifIp(xapi, pif, NETWORK_PREFIX + (hostIpLastNumber++))))
      console.log('newNetwork', xosanNetwork.$PIFs)
      let sshKey = xapi.xo.getData(xapi.pool, 'xosan_ssh_key')
      if (!sshKey) {
        try {
          await fs.access(SSH_KEY_FILE, fs.constants.R_OK)
        } catch (e) {
          await runCmd('ssh-keygen', [ '-q', '-f', SSH_KEY_FILE, '-t', 'rsa', '-b', '4096', '-N', '' ])
        }
        sshKey = {
          private: await fs.readFile(SSH_KEY_FILE, 'ascii'),
          public: await fs.readFile(SSH_KEY_FILE + '.pub', 'ascii')
        }
        xapi.xo.setData(xapi.pool, 'xosan_ssh_key', sshKey)
      }
      const public_key = sshKey.public
      const private_key = sshKey.private
      const srsObjects = map(srs, srId => xapi.getObject(srId))

      const vmParameters = map(srs, srId => {
        const sr = xapi.getObject(srId)
        const host = xapi.getObject(xapi.getObject(sr.$PBDs[ 0 ]).host)
        return {
          sr,
          host,
          name_label: 'XOSAN - ' + sr.name_label + ' - ' + host.name_label,
          name_description: 'Xosan VM storing data on volume ' + sr.name_label,
          // the values of the xenstore_data object *have* to be string, don't forget.
          xenstore_data: {
            'vm-data/hostname': 'XOSAN' + sr.name_label,
            'vm-data/sshkey': public_key,
            'vm-data/ip': NETWORK_PREFIX + (vmIpLastNumber++),
            'vm-data/mtu': String(xosanNetwork.MTU),
            'vm-data/vlan': String(vlan)
          }
        }
      })
      await Promise.all(vmParameters.map(vmParam => callPlugin(xapi, vmParam.host, 'receive_ssh_keys', {
        private_key,
        public_key,
        force: 'true'
      })))
      console.log('network VIFS: ', xosanNetwork.$VIFs)
      console.log('network PIFS: ', xosanNetwork.$PIFs, xosanNetwork.$PIFs.map(pif => pif.$metrics))
      const firstVM = await importVM(xapi, vmParameters[ 0 ].sr)
      await xapi.editVm(firstVM, {
        autoPoweron: true
      })
      const vmsAndParams = [ {
        vm: firstVM,
        params: vmParameters[ 0 ]
      } ].concat(await Promise.all(vmParameters.slice(1).map(param => copyVm(xapi, firstVM, param))))

      const ipAndHosts = await Promise.all(map(vmsAndParams, vmAndParam => prepareGlusterVm(xapi, vmAndParam, xosanNetwork)))
      const firstIpAndHost = ipAndHosts[ 0 ]
      for (let i = 1; i < ipAndHosts.length; i++) {
        console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster peer probe ' + ipAndHosts[ i ].address))
      }
      const volumeCreation = 'gluster volume create xosan disperse ' + ipAndHosts.length +
        ' redundancy 1 ' + ipAndHosts.map(ipAndHosts => (ipAndHosts.address + ':/bricks/xosan/xosandir')).join(' ')
      console.log('creating volume: ', volumeCreation)
      console.log(await remoteSsh(xapi, firstIpAndHost, volumeCreation))
      console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan group virt'))
      console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan features.shard on'))
      console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan features.shard-block-size 16MB'))
      console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume set xosan performance.stat-prefetch on'))
      console.log(await remoteSsh(xapi, firstIpAndHost, 'gluster volume start xosan'))
      console.log('xosan gluster volume started')
      const config = { server: firstIpAndHost.address + ':/xosan' }
      const xosanSr = await xapi.call('SR.create', srsObjects[ 0 ].$PBDs[ 0 ].$host.$ref, config, 0, 'XOSAN', 'XOSAN', 'xosan', '', true, {})
      console.log('xosan_hosts0', vmParameters.map(param => param.host.$id))
      await xapi.xo.setData(xosanSr, 'xosan_config', ipAndHosts.map(param => ({
        host: param.host.$id,
        vm: { id: param.vm.$id, ip: param.address }
      })))
      await xapi.xo.setData(xapi.pool, 'xosan_sr', xosanSr.$id)
    }
  } catch (e) {
    console.log(e)
  }
}

createVM.description = 'create gluster VM'
createVM.permission = 'admin'
createVM.params = {
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
  }
}

createVM.resolve = {
  srs: [ 'sr', 'SR', 'administrate' ],
  pif: [ 'pif', 'PIF', 'administrate' ]
}
