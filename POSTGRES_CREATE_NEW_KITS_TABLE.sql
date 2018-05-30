DROP TABLE doarni.new_kits_to_track;
CREATE TABLE doarni.new_kits_to_track
(
    kit_product_number VARCHAR(128) NOT NULL,
    kit_serial_number NUMERIC,
    kit_product_id BIGINT,
    kit_serial_id BIGINT,
    kit_stock_id INTEGER
);

CREATE UNIQUE INDEX new_kits_to_track_kit_stock_id_pk ON doarni.new_kits_to_track (kit_stock_id);
CREATE INDEX new_kits_to_track_kit_product_number_index ON doarni.new_kits_to_track (kit_product_number);
COMMENT ON TABLE doarni.new_kits_to_track IS 'Used to store kits and their serials to be tracked';
GRANT SELECT ON TABLE doarni.new_kits_to_track TO reader;