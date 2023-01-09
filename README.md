
sudo -u postgres psql

create database tradellama;
create user tradellama;
grant all privileges on database tradellama to tradellama; 
alter user tradellama with encrypted password 'puppyjuice06!';

grant all privileges on file_summaries to tradellama; 
grant all privileges on file_summaries_id_seq to tradellama; 
grant all privileges on account_summaries to tradellama; 
grant all privileges on account_summaries_id_seq to tradellama; 
grant all privileges on security_summaries to tradellama; 
grant all privileges on security_summaries_id_seq to tradellama; 
grant all privileges on chains to tradellama; 
grant all privileges on chains_id_seq to tradellama; 


make sure this is running

flyctl proxy 15432:5432 -a green-feather-3408-db


fly postgres connect -a green-feather-3408-db

create user tradellama;
grant all privileges on database green_feather_3408 to tradellama;
alter user tradellama with encrypted password 'puppyjuice06!';

psql -d "host=localhost port=15432 dbname=green_feather_3408 user=tradellama"


