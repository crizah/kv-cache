Compare SGlangs L3 storage and us
We test metrics on NodeA and NodeB transfers. NodeA churning its own requests, and NodeB pulling
We test for different bytes moved aka diff prefix lengths 
(we can have the test suite 2048,8192,16384,24576) (because i am tired)

Setup being A is 80% and 100% saturation and its churning its own req

Metrics:
On NodeB:
- TTFT
- effective speed
On NodeA:
- TPOT p95, against its churn baseline GPU utlisation, CPU utilisation

the file storage can just be one of the hosts' folders
launch both servers with `--hicache-storage-backend file` and `--hicache-write-policy write_through`

run with the 25 mps percentage
