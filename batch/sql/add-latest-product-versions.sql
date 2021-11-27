CREATE TABLE IF NOT EXISTS `latest_product_versions` (
  `product` VARCHAR(100) NOT NULL,
  `version` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`product`)
) ENGINE = InnoDB;
