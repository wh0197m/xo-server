import fromPairs from 'lodash/fromPairs'
import find from 'lodash/find'
import filter from 'lodash/filter'
import map from 'lodash/map'
import arp from 'arp-a'
import { spawn } from 'child_process'
import { createReadStream } from 'fs'

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

export async function getVolumeInfo ({ ip, volumeName }) {
  let giantIPtoVMDict = fromPairs([].concat.apply([], map(this.getAllXapis(), xapi => {
    let collected = filter(xapi.objects.all, { $type: 'vm' })
      .map(vm => (vm.$guest_metrics ? Object.values(vm.$guest_metrics.networks).map(ip => ([ ip, vm.$id ])) : []))
    return [].concat.apply([], collected)
  })))
  // ssh -o StrictHostKeyChecking=no  root@192.168.0.201 gluster volume info xosan
  const result = await runSsh(ip, 'gluster volume info ' + volumeName)
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
  info.peers = await getPeers({ ip: ip })
  return info
}

getVolumeInfo.description = 'info on gluster volume'
getVolumeInfo.permission = 'admin'

getVolumeInfo.params = {
  ip: {
    type: 'string'
  },
  volumeName: {
    type: 'string'
  }
}

function trucate2048 (value) {
  return 2048 * Math.floor(value / 2048)
}

async function importVM (xapi, sr) {
  let stream = createReadStream('../XOSANTEMPLATE.xva')
  return await xapi.importVm(stream, { srId: sr.$ref, type: 'xva' })
}

async function prepareGlusterVm (xapi, vm, sr) {
  //refresh the object so that sizes are correct
  sr = xapi.getObject(sr.$id)
  let xosanNetwork = find(xapi.objects.all, obj => (obj.$type === 'network' && xapi.xo.getData(obj, 'xosan')))
  await xapi._waitObjectState(sr.$id, sr => Boolean(sr.$PBDs))
  let host = xapi.getObject(xapi.getObject(sr.$PBDs[ 0 ]).host)
  let firstVif = vm.$VIFs[ 0 ]
  if (xosanNetwork.$id !== firstVif.$network.$id) {
    console.log('VIF in wrong network (' + firstVif.$network.name_label + '), moving to correct one: ' + xosanNetwork.name_label)
    await xapi.call('VIF.move', firstVif.$ref, xosanNetwork.$ref)
  }
  await xapi.editVm(vm, {
    name_label: 'XOSAN - ' + sr.name_label + ' - ' + host.name_label,
    name_description: 'Xosan VM storing data on volume ' + sr.name_label
  })
  const dataDisk = vm.$VBDs.map(vbd => vbd.$VDI).find(vdi => vdi && vdi.name_label === 'xosan_data')
  const srFreeSpace = sr.physical_size - sr.physical_utilisation
  //we use a percentage because it looks like the VDI overhead is proportional
  const newSize = trucate2048((srFreeSpace + dataDisk.virtual_size) * 0.98)
  await xapi._resizeVdi(dataDisk, newSize)
  await xapi.startVm(vm)
  vm = await xapi._waitObjectState(vm.$id, vm => Boolean(vm.$guest_metrics) && Boolean(Object.values(vm.$guest_metrics.networks).length))
  const networks = vm.$guest_metrics.networks
  /* expected:
   { '0/ip': '192.168.0.56',
   '0/ipv6/0': 'fe80::bcb0:6366:3670:ae42',
   '0/ipv6/1': 'fe80::14ed:30ab:44b9:6c0c',
   '0/ipv6/2': 'fe80::96c6:d82f:db1a:486e' }
   */
  // trying to match on IPv4 addresses. Is IPv6 discrimination bad?
  const key = Object.keys(networks).find(key => key.match(/0\/ip($|\/)/))
  const address = networks[ key ]
  await runCmd('sshpass', [ '-p', 'qwerty', 'ssh-copy-id', '-o', 'StrictHostKeyChecking=no', 'root@' + address ])
  await runSsh(address, [ 'passwd -l root' ])
  return address
}

export async function createVM ({ srs }) {
  if (srs.length > 0) {
    let xapi = find(this.getAllXapis(), xapi => (xapi.getObject(srs[ 0 ])))
    const srsObjects = map(srs, srId => xapi.getObject(srId))
    const firstVM = await importVM(xapi, srsObjects[ 0 ])
    const vmsAndSrs = [ { vm: firstVM, sr: srsObjects[ 0 ] } ]
    for (let i = 1; i < srsObjects.length; i++) {
      vmsAndSrs.push({ vm: await xapi.copyVm(firstVM, srsObjects[ i ]), sr: srsObjects[ i ] })
    }
    const ipAddresses = await Promise.all(map(vmsAndSrs, vmAndSr => prepareGlusterVm(xapi, vmAndSr.vm, vmAndSr.sr)))
    console.log('ipAddresses returned', ipAddresses)
    const firstAddress = ipAddresses[ 0 ]
    for (let i = 1; i < ipAddresses.length; i++) {
      console.log(await runSsh(firstAddress, [ 'gluster peer probe ' + ipAddresses[ i ] ]))
    }

    const volumeCreation = 'gluster volume create xosan disperse ' + ipAddresses.length +
      ' redundancy 1 ' + ipAddresses.map(ip => (ip + ':/bricks/xosan/xosandir')).join(' ')
    console.log('creating volume', volumeCreation)
    console.log(await runSsh(firstAddress, [ volumeCreation ]))
    console.log(await runSsh(firstAddress, [ 'gluster volume set xosan group virt' ]))
    console.log(await runSsh(firstAddress, [ 'gluster volume set xosan features.shard on' ]))
    console.log(await runSsh(firstAddress, [ 'gluster volume set xosan features.shard-block-size 16MB' ]))
    console.log(await runSsh(firstAddress, [ 'gluster volume set xosan performance.stat-prefetch on' ]))
    console.log(await runSsh(firstAddress, [ 'gluster volume start xosan' ]))
    console.log('xosan gluster volume started')
    const config = { server: firstAddress + ':/xosan' }
    await xapi.call('SR.create', srsObjects[ 0 ].$PBDs[ 0 ].$host.$ref, config, 0, 'XOSAN', 'XOSAN', 'xosan', '', true, {})
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
  }
}

createVM.resolve = {
  srs: [ 'sr', 'SR', 'administrate' ]
}
