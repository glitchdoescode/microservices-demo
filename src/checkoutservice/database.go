package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice/genproto"
	_ "github.com/lib/pq"
)

type Database struct {
	db *sql.DB
}

// InitDatabase initializes PostgreSQL connection
func InitDatabase() (*Database, error) {
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "postgres"
	}

	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "orders_user"
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "orders_password"
	}

	dbname := os.Getenv("POSTGRES_DB")
	if dbname == "" {
		dbname = "orders_db"
	}

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	log.Infof("Connecting to PostgreSQL at %s:%s", host, port)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Test connection with retries
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		err = db.Ping()
		if err == nil {
			log.Info("Successfully connected to PostgreSQL")
			break
		}
		log.Warnf("Failed to connect to PostgreSQL (attempt %d/%d): %v", i+1, maxRetries, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database after %d attempts: %v", maxRetries, err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	database := &Database{db: db}

	// Create tables if they don't exist
	if err := database.createTables(); err != nil {
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return database, nil
}

// createTables creates orders and order_items tables if they don't exist
func (d *Database) createTables() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create orders table
	ordersTable := `
	CREATE TABLE IF NOT EXISTS orders (
		id VARCHAR(36) PRIMARY KEY,
		user_id VARCHAR(255) NOT NULL,
		user_email VARCHAR(255) NOT NULL,
		shipping_tracking_id VARCHAR(255),
		shipping_cost_currency VARCHAR(10),
		shipping_cost_units BIGINT,
		shipping_cost_nanos INTEGER,
		shipping_address_street VARCHAR(255),
		shipping_address_city VARCHAR(255),
		shipping_address_state VARCHAR(255),
		shipping_address_country VARCHAR(255),
		shipping_address_zip_code VARCHAR(20),
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_user_id (user_id),
		INDEX idx_created_at (created_at)
	);`

	if _, err := d.db.ExecContext(ctx, ordersTable); err != nil {
		return fmt.Errorf("failed to create orders table: %v", err)
	}

	// Create order_items table
	orderItemsTable := `
	CREATE TABLE IF NOT EXISTS order_items (
		id SERIAL PRIMARY KEY,
		order_id VARCHAR(36) NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
		product_id VARCHAR(255) NOT NULL,
		quantity INTEGER NOT NULL,
		cost_currency VARCHAR(10),
		cost_units BIGINT,
		cost_nanos INTEGER,
		INDEX idx_order_id (order_id),
		INDEX idx_product_id (product_id)
	);`

	if _, err := d.db.ExecContext(ctx, orderItemsTable); err != nil {
		return fmt.Errorf("failed to create order_items table: %v", err)
	}

	log.Info("Database tables verified/created successfully")
	return nil
}

// SaveOrder saves order data to PostgreSQL
func (d *Database) SaveOrder(ctx context.Context, orderResult *pb.OrderResult, userEmail string, userId string) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Insert order
	orderQuery := `
		INSERT INTO orders (
			id, user_id, user_email, shipping_tracking_id,
			shipping_cost_currency, shipping_cost_units, shipping_cost_nanos,
			shipping_address_street, shipping_address_city, shipping_address_state,
			shipping_address_country, shipping_address_zip_code
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`

	var shippingStreet, shippingCity, shippingState, shippingCountry, shippingZip string
	if orderResult.ShippingAddress != nil {
		shippingStreet = orderResult.ShippingAddress.StreetAddress
		shippingCity = orderResult.ShippingAddress.City
		shippingState = orderResult.ShippingAddress.State
		shippingCountry = orderResult.ShippingAddress.Country
		shippingZip = orderResult.ShippingAddress.ZipCode
	}

	var currency string
	var units int64
	var nanos int32
	if orderResult.ShippingCost != nil {
		currency = orderResult.ShippingCost.CurrencyCode
		units = orderResult.ShippingCost.Units
		nanos = orderResult.ShippingCost.Nanos
	}

	_, err = tx.ExecContext(ctx, orderQuery,
		orderResult.OrderId, userId, userEmail, orderResult.ShippingTrackingId,
		currency, units, nanos,
		shippingStreet, shippingCity, shippingState, shippingCountry, shippingZip,
	)
	if err != nil {
		return fmt.Errorf("failed to insert order: %v", err)
	}

	// Insert order items
	itemQuery := `
		INSERT INTO order_items (
			order_id, product_id, quantity, cost_currency, cost_units, cost_nanos
		) VALUES ($1, $2, $3, $4, $5, $6)
	`

	for _, item := range orderResult.Items {
		var itemCurrency string
		var itemUnits int64
		var itemNanos int32
		if item.Cost != nil {
			itemCurrency = item.Cost.CurrencyCode
			itemUnits = item.Cost.Units
			itemNanos = item.Cost.Nanos
		}

		_, err = tx.ExecContext(ctx, itemQuery,
			orderResult.OrderId,
			item.GetItem().GetProductId(),
			item.GetItem().GetQuantity(),
			itemCurrency, itemUnits, itemNanos,
		)
		if err != nil {
			return fmt.Errorf("failed to insert order item: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	log.Infof("Order %s saved to database successfully", orderResult.OrderId)
	return nil
}

// Close closes the database connection
func (d *Database) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}
