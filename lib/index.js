var lmdb = require('node-lmdb');

function localdb(opts) {
  var self = this;

  self.env = new lmdb.Env();
  self.path = opts.path;
  self.opts = opts.opts || {};
  self.start = opts.start;
  self.end = opts.end;
  if(!self.path){
    throw new Error("'path' is a required option");
  }
  if(!self.start){
    throw new Error("'start' is a required option");
  }
  if(!self.end){
    throw new Error("'end' is a required option");
  }

}

localdb.prototype.open = function(cb) {
  var self = this;

  var env = self.env;
  var opts = self.opts;
  opts.path = self.path;

  env.open(opts);
  self.ips = env.openDbi({
    name: "ips",
    create: true
  });
  self.macs = env.openDbi({
    name: "macs",
    create: true
  });
  cb();
};

localdb.prototype.close = function(cb) {
  this.env.close();
};

localdb.prototype.save_lease = function save_lease (lease, cb) {
  var self;
  var mac_addr = lease.chaddr;

  var txn = self.env.beginTxn();
  txn.putString(self.macs, mac_addr, JSON.stringify(lease));
  txn.putString(self.ips, lease.yiaddr, mac_addr);
  txn.commit();
  cb();
};

localdb.prototype.get_lease = function get_lease (mac_addr, cb) {
  var self = this;

  var txn = self.env.beginTxn();
  var lease = txn.getString(self.macs, mac_addr);
  txn.commit();

  if(!lease){
    return cb(null);
  }

  cb(JSON.parse(lease));
};

localdb.prototype.get_lease_by_ip = function get_lease_by_ip (ip, cb) {
  var self = this;

  var txn = self.env.beginTxn();
  var mac_addr = txn.getString(self.ips, ip);
  var lease = txn.getString(self.macs, mac_addr);
  txn.commit();

  if(!lease){
    return cb(null);
  }
  cb(JSON.parse(lease));
};

localdb.prototype.remove_lease = function remove_lease (mac_addr, cb) {
  var self = this;

  var txn = self.env.beginTxn();
  var lease = txn.getString(self.macs, mac_addr);
  var ip = lease.yiaddr;
  txn.del(self.macs, mac_addr);
  txn.del(self.ips, ip);
  txn.commit();

  cb();
};

function long2ip(ip) {
  if (!isFinite(ip))
    return false;
  return '' + [ip >>> 24, ip >>> 16 & 0xFF, ip >>> 8 & 0xFF, ip & 0xFF].join('.');
}

function ip2long (ip_address) {
  var parts = ip_address.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  return parts.length == 5 ?
      parseInt(+parts[1],10) * 16777216 + parseInt(+parts[2],10) * 65536 +
      parseInt(+parts[3],10) * 256      + parseInt(+parts[4],10) * 1
    : false;
}

localdb.prototype.get_next_ip = function get_next_ip (cb) {
  var self = this;
  var ip_list = [];

  var txn = env.beginTxn();
  var cursor = new lmdb.Cursor(txn, self.ips);
  while(cursor.goToNext()){
    ip_list.push(cursor.getCurrentString());
  }
  cursor.close();
  txn.commit();

  var sorted_list = ip_list.sort(function(a, b) {
    return (ip2long(a) - ip2long(b));
  });

  var latest = sorted_list[-1];
  if(!latest) {
    return cb(self.start);
  }
  var next_ip = (ip2long(latest) + 1);
  if(next_ip <= ip2long(this.end)){
    return cb(long2ip(next_ip));
  } else {
    return cb(undefined);
  }
};

module.exports = localdb;
