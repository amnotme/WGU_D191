-- Step 0. Set up environment. i.e. Delete triggers, stored_procedures, and tables.

DROP TABLE IF EXISTS detail_table;
DROP TABLE IF EXISTS summary_table;
DROP FUNCTION IF EXISTS update_summary_table;
DROP FUNCTION IF EXISTS add_premium_to_rental_rate;
DROP FUNCTION IF EXISTS extract_month_year;
DROP PROCEDURE IF EXISTS clear_and_repopulate_detail_and_summary_table;
-- Step 1. Create Detail and Summary Tables.

CREATE TABLE IF NOT EXISTS detail_table (
	detail_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	inventory_id integer NOT NULL,
	film_id integer NOT NULL,
	rental_id integer NOT NULL,
	title text NOT NULL,
	rental_rate numeric NOT NULL,
	premium_rate numeric NOT NULL,
	month_year text NOT NULL,
	category_id integer NOT NULL,
	category_name text NOT NULL
);

CREATE TABLE IF NOT EXISTS summary_table (
	id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	rank_id integer NOT NULL,
	category_name text NOT NULL,
	movie_title text NOT NULL,
	new_rental_rate numeric NOT NULL
);

-- Step 2. Create functions to transform data columns selected
-- -- Add premium to movies.

CREATE OR REPLACE FUNCTION add_premium_to_rental_rate(rental_rate numeric)
RETURNS numeric AS $$
DECLARE
   new_rental_rate numeric;
   premium numeric;
 BEGIN
   premium := 0.5;
   new_rental_rate := rental_rate + premium;
   RETURN new_rental_rate;
 END;
$$ LANGUAGE plpgsql;

-- -- Extract month and year from timestamp

CREATE OR REPLACE FUNCTION extract_month_year(rental_date timestamp)
RETURNS text AS $$
BEGIN
  RETURN to_char(rental_date, 'MM/YYYY');
  END;
$$ LANGUAGE plpgsql;

-- Step 3. Pull relevant information needed to populate detail_table and summary_table
-- -- Populate detail_table

INSERT INTO detail_table (
	inventory_id,
	film_id,
	rental_id,
	title,
	rental_rate,
	premium_rate,
	month_year,
	category_id,
	category_name
)
SELECT 
	INV.inventory_id, 
	FI.film_id, 
	RE.rental_id, 
	FI.title, 
	FI.rental_rate, 
	add_premium_to_rental_rate(FI.rental_rate) as premium_rate, 
	extract_month_year(RE.rental_date) as month_year, 
	CA.category_id, 
	CA.name AS category_name
FROM Film AS FI
INNER JOIN Inventory AS INV
	ON INV.film_id = FI.film_id
INNER JOIN rental AS RE
	ON RE.inventory_id = INV.inventory_id
INNER JOIN film_category AS FI_CA
	ON FI_CA.film_id = FI.film_id
INNER JOIN category AS CA
	ON CA.category_id = FI_CA.category_id

SELECT * FROM detail_table;

-- -- Populate summary table
INSERT INTO summary_table (
	rank_id,
	category_name,
	movie_title,
	new_rental_rate
)
SELECT 
	row_num AS rank_id,
	category_name, 
	title AS movie_title, 
	premium_rate AS new_rental_rate
FROM (
  SELECT 
	COUNT(*) AS number_of_rentals, 
	title, 
	premium_rate, 
	category_name,
    ROW_NUMBER() 
		OVER (
			PARTITION BY category_name ORDER BY COUNT(*) DESC
		) AS row_num
  	FROM detail_table
  GROUP BY title, premium_rate, category_name
) AS subquery
WHERE row_num <= 10;

SELECT * FROM summary_table;

-- Step 4. Create trigger to update summary_table when detail table is updated.
-- -- Create the update_summary_table trigger function

CREATE OR REPLACE FUNCTION update_summary_table()
RETURNS TRIGGER AS $$
BEGIN
    -- Delete existing records for the affected category
    DELETE FROM summary_table
    WHERE category_name = NEW.category_name;
    
    -- Insert new records for the affected category
    INSERT INTO summary_table (rank_id, category_name, movie_title, new_rental_rate)
    SELECT row_num AS rank_id, category_name, title AS movie_title, premium_rate AS new_rental_rate
    FROM (
      SELECT 
        COUNT(*) AS number_of_rentals, 
        title, 
        premium_rate, 
        category_name,
        ROW_NUMBER() 
            OVER (
                PARTITION BY category_name ORDER BY COUNT(*) DESC
            ) AS row_num
      FROM detail_table
      WHERE category_name = NEW.category_name
      GROUP BY title, premium_rate, category_name
    ) AS subquery
    WHERE row_num <= 10;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- -- Create the trigger
DROP TRIGGER update_summary_trigger ON detail_table;
CREATE TRIGGER update_summary_trigger
AFTER INSERT ON detail_table
FOR EACH ROW
EXECUTE FUNCTION update_summary_table();

-- Step 5. Let's check that our trigger to update summary table works.
-- -- Let's check the detail table and select movie title that have a ranking set.

SELECT * FROM summary_table;

-- -- Top 2 from Action category.
SELECT * FROM detail_table where Title like 'Rugrats Shakespeare' AND category_name like 'Action'
SELECT * FROM detail_table where Title like 'Suspects Quills' AND category_name like 'Action'

-- -- Get the number of movies rented for some known movies..
SELECT COUNT(*) FROM detail_table where Title like 'Rugrats Shakespeare' AND category_name like 'Action' --26
SELECT COUNT(*) FROM detail_table where Title like 'Suspects Quills' AND category_name like 'Action' --21

-- -- Insert a couple more and check if summary table has been updated.
INSERT INTO detail_table (inventory_id, film_id, rental_id, title, rental_rate, premium_rate, month_year, category_id, category_name)
VALUES
(3990, 869, 100001, 'Suspects Quills', 2.99, 3.49, '05/2005', 1, 'Action' )

SELECT * FROM summary_table
ORDER BY category_name

-- -- Insert a couple more and check if summary table has been updated.
INSERT INTO detail_table (inventory_id, film_id, rental_id, title, rental_rate, premium_rate, month_year, category_id, category_name)
VALUES
(3418, 748, 100001, 'Rugrats Shakespeare', 0.99, 1.49, '05/2005', 1, 'Action' )

SELECT * FROM summary_table
ORDER BY category_name


-- Step 6. Create the stored procedure that we can call to refresh the detail and summary table.
CREATE OR REPLACE PROCEDURE clear_and_repopulate_detail_and_summary_table()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Clear the detail_table
    DELETE FROM detail_table;
	-- Clear the summary_table
	DELETE FROM summary_table;
	
    -- Repopulate the detail_table
    INSERT INTO detail_table (
        inventory_id,
        film_id,
        rental_id,
        title,
        rental_rate,
        premium_rate,
        month_year,
        category_id,
        category_name
    )
    SELECT 
        INV.inventory_id, 
        FI.film_id, 
        RE.rental_id, 
        FI.title, 
        FI.rental_rate, 
        add_premium_to_rental_rate(FI.rental_rate) as premium_rate, 
        extract_month_year(RE.rental_date) as month_year, 
        CA.category_id, 
        CA.name AS category_name
    FROM Film AS FI
    INNER JOIN Inventory AS INV
        ON INV.film_id = FI.film_id
    INNER JOIN rental AS RE
        ON RE.inventory_id = INV.inventory_id
    INNER JOIN film_category AS FI_CA
        ON FI_CA.film_id = FI.film_id
    INNER JOIN category AS CA
        ON CA.category_id = FI_CA.category_id;     
	-- We have a trigger on the detail_table that will repopulate the summary_table
END;
$$;

-- -- call the store procedure.
CALL clear_and_repopulate_detail_and_summary_table();

-- -- Ensure that the tables have been refreshed.
SELECT * FROM detail_table
SELECT * FROM summary_table


