
sudo -u postgres psql

create database tradellama;
create user tradellama;
grant all privileges on database tradellama to tradellama; 
alter user tradellama with encrypted password 'puppyjuice06!';

