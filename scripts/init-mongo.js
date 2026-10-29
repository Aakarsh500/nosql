// Initiate a single-node replica set (rs0). MongoDB transactions need a replica set.
try {
  rs.status();
  print('Replica set already active');
} catch (error) {
  rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'mongo:27017'}]});
  print('Replica set initiated');
}
for (let attempt = 0; attempt < 60; attempt++) {
  if (db.hello().isWritablePrimary) break;
  sleep(1000);
}
if (!db.hello().isWritablePrimary) throw new Error('MongoDB primary not ready');
print('MongoDB primary ready');
