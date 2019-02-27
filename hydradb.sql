
-- sqlcmd -S myServer\instanceName -i C:\myScript.sql

-- createdb hf
create database hydradb;
-- and change to the hydradb
\c hydradb

-- enable postgis extension for db
create extension postgis;

-- create static dataset schema
create schema static;
create schema obs;
set search_path to public, static, obs;

-- enable postgis for schemas
alter extension postgis
  set schema static;
alter extension postgis
  set schema obs;

-- create table for hand dataset in static schema
create table hydradb.static.hand (rid serial primary key, rast raster);

-- create table for permenant water in static schema
create table hydradb.static.pemwater(rid serial primary key, rast raster);

-- create landsat table with band and date info
create table hydradb.obs.landsat (rid serial primary key, rast raster, band text not null, ftime timestamp not null);
create index landsat_t on landsat(ftime);
create index landsat_b on landsat(band);

  -- create modis table with band and date info
create table hydradb.obs.modis(rid serial primary key, rast raster, band text not null, ftime timestamp not null);
create index modis_t on modis(ftime);
create index modis_b on modis(band);

  -- create viirs table with band and date info
create table hydradb.obs.viirs (rid serial primary key, rast raster, band text not null, ftime timestamp not null);
create index viirs_t on viirs(ftime);
create index viirs_b on viirs(band);

  -- create viirs table with band and date info
create table hydradb.obs.atms (rid serial primary key, rast raster, band text not null, ftime timestamp not null);
create index atms_t on atms(ftime);
create index atms_b on atms(band);
