-- init_dw/01_init.sql
CREATE TABLE IF NOT EXISTS ventas(
    id SERIAL PRIMARY KEY,
    invoiceno VARCHAR(50),
    stockcode VARCHAR(50),
    description VARCHAR(255),
    quantity INTEGER,
    invoicedate TIMESTAMP,
    unitprice DECIMAL(10, 2),
    customerid VARCHAR(50),
    country VARCHAR(50),
    netrevenue DECIMAL(10, 2),
    category VARCHAR(100),
    bruterevenue DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS devoluciones (
    id SERIAL PRIMARY KEY,
    invoiceno VARCHAR(50),
    description VARCHAR(255),
    stockcode VARCHAR(50),
    quantity INTEGER,
    unitprice DECIMAL(10, 2),
    customerid VARCHAR(50),
    country VARCHAR(50),
    invoicedate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS log_rechazos (
    id SERIAL PRIMARY KEY,
    invoiceno VARCHAR(50),
    stockcode VARCHAR(50),
    description VARCHAR(255),
    quantity INTEGER,
    invoicedate TIMESTAMP,
    unitprice DECIMAL(10, 2),
    customerid VARCHAR(50),
    country VARCHAR(50),
    motivo varchar(255),
    fuente varchar(255)
);