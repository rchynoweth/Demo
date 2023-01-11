-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS rac_demo_db
    COMMENT "CREATE A DATABASE WITH A LOCATION PATH"
    LOCATION "/Users/ryan.chynoweth@databricks.com/databases/rac_demo_db_silver" --this must be a location on dbfs (i.e. not direct access)
     
