DROP TABLE doarni.new_kit_progress_tracker;
CREATE TABLE doarni.new_kit_progress_tracker
(
    kit_barcode VARCHAR(128) NOT NULL,
    kit_product_number VARCHAR(128) NOT NULL,
    kit_serial_number NUMERIC NOT NULL,
    kit_bin VARCHAR(128) NOT NULL,
    kit_health VARCHAR(128),
    potential_action_on_kit VARCHAR(128),
    kit_container_id BIGINT,
    kit_serial_id BIGINT,
    kit_product_id BIGINT,
    kit_stock_id BIGINT
);
CREATE UNIQUE INDEX new_kit_progress_tracker_kit_stock_id_pk ON doarni.new_kit_progress_tracker (kit_stock_id);
CREATE INDEX new_kit_progress_tracker_kit_product_number_index ON doarni.new_kit_progress_tracker (kit_product_number);
COMMENT ON TABLE doarni.new_kit_progress_tracker IS 'Used to track progress on new kits added to southaven';
GRANT SELECT ON TABLE doarni.new_kit_progress_tracker TO reader;