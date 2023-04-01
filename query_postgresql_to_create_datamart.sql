# to execute this query using postgresql, before exceute you must exceute insert_to_postgresql.py to create table penjualan, barang, pelanggan, dll
ALTER TABLE penjualan 
ADD CONSTRAINT FK_Penjualan_Pelanggan
FOREIGN KEY (id_customer) REFERENCES pelanggan(id_customer);

ALTER TABLE penjualan 
ADD CONSTRAINT FK_Penjualan_Barang
FOREIGN KEY (id_barang) REFERENCES barang(id_barang);

CREATE INDEX id_bar_cus
ON penjualan(id_customer, id_barang);

# you can use this query to create staging table base
CREATE table  staging_rakamin.base_table2 as select p.id_invoice, p.tanggal, p2.id_customer, p2."level", p2.nama, p2.id_group, p2.grup, 
b.id_barang, b.nama_barang, b.tipe, b.nama_tipe, p.id_brand, b.kode_brand, p.brand, b.kemasan, p.unit, p.id_distributor,
p.jumlah_barang, p.harga, p.mata_uang
from penjualan p 
join pelanggan p2 on p.id_customer = p2.id_customer 
join barang b on b.id_barang = p.id_barang 

# this query for create table datamart
create table datamart_rakamin.invoice as 
with belum_total as (select tanggal, nama as nama_customer, nama_barang, brand, harga, jumlah_barang, 
sum(harga*jumlah_barang) as jumlah 
from base_table bt group by tanggal, nama, nama_barang, brand, harga, jumlah_barang) 
select tanggal, nama_customer, nama_barang, brand, harga, jumlah_barang, jumlah as sub_total, 
SUM(jumlah) OVER(PARTITION BY tanggal, nama_customer, nama_barang) as total 
from belum_total 
