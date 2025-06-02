-- 1. Products table
CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    supplier_id INT,
    reorder_level INT DEFAULT 10,
    CONSTRAINT fk_supplier FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- 2. Warehouses table
CREATE TABLE warehouses (
    warehouse_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100)
);

-- 3. Stock Movements table
CREATE TABLE stock_movements (
    movement_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    warehouse_id INT,
    quantity INT NOT NULL,
    movement_type ENUM('IN', 'OUT') NOT NULL,
    movement_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT fk_warehouse FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);

-- 4. Suppliers table
CREATE TABLE suppliers (
    supplier_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    contact_email VARCHAR(100)
);


-- CRUD OPERATIONS
INSERT INTO products (name, description, supplier_id, reorder_level)
VALUES ('Hammer', 'Heavy duty', 1, 5);

INSERT INTO stock_movements (product_id, warehouse_id, quantity, movement_type)
VALUES (1, 1, 50, 'IN');

SELECT 
    product_id,
    SUM(CASE WHEN movement_type = 'IN' THEN quantity ELSE -quantity END) AS current_stock
FROM stock_movements
GROUP BY product_id;

DELETE FROM products WHERE product_id = 1;

-- STORED PROCEDURES
DELIMITER $$

CREATE PROCEDURE GetLowStockProducts()
BEGIN
    SELECT p.product_id, p.name,
        COALESCE(SUM(CASE WHEN sm.movement_type = 'IN' THEN sm.quantity ELSE -sm.quantity END), 0) AS current_stock,
        p.reorder_level
    FROM products p
    LEFT JOIN stock_movements sm ON p.product_id = sm.product_id
    GROUP BY p.product_id, p.name, p.reorder_level
    HAVING current_stock < p.reorder_level;
END $$

DELIMITER ;


--create index
CREATE INDEX idx_product_id ON stock_movements(product_id);
CREATE INDEX idx_warehouse_id ON stock_movements(warehouse_id);

