#DSP4: Consistent Hashing-based Naming Service
This project we have implemented a consistent hashing (CH)-based flat naming system. Consistent hashing provides a lookup service for key-value pairs. The CH system stores (key, value) pairs on a distributed set of servers. These servers collaboratively provide lookup service along with insertion and deletion. In this program, we implemented the CH name servers and a boot-strap CH server, which is a special CH server with certain extra functionalities.

## Several assumptions are made as follows:
1. The keys are assumed to be numbers in the range [0, 1023] 
2. Values are assumed to be alphanumeric strings 
3. The CH name server IDs are also numbers in [0, 1023] range.
4. The Bootstrap name server has the ID 0. 
5. The lookup, insertion and deletion operations are executed only at the Bootstrap name server.
6. The name servers interact with one another using sockets primitive.
