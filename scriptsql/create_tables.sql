CREATE TABLE IF NOT EXISTS public.dim_product
(
    stock_code character(50),
    description text,
    PRIMARY KEY (stock_code)
);

CREATE TABLE IF NOT EXISTS public.dim_customer
(
    customer_id integer,
    country character(50),
    PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS public.dim_date
(
    date_id serial,
    date date,
    year integer,
    month integer,
    day integer,
    weekday character(50),
    quarter integer,
    PRIMARY KEY (date_id)
);

CREATE TABLE IF NOT EXISTS public.fact_sales
(
    sales_id serial,
    invoice_no character(50),
    stock_code character(50),
    customer_id integer,
    date_id integer,
    quantity integer,
    unit_price numeric,
    PRIMARY KEY (sales_id)
);

ALTER TABLE IF EXISTS public.fact_sales
    ADD FOREIGN KEY (stock_code)
    REFERENCES public.dim_product (stock_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.fact_sales
    ADD FOREIGN KEY (customer_id)
    REFERENCES public.dim_customer (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.fact_sales
    ADD FOREIGN KEY (date_id)
    REFERENCES public.dim_date (date_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
