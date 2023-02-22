DELIMITER $$

DROP TRIGGER IF EXISTS resources_before_insert $$
CREATE TRIGGER resources_before_insert BEFORE INSERT ON resources
FOR EACH ROW
BEGIN
  DECLARE last_id INT;
  SELECT COALESCE(MAX(resource_id), 0) INTO last_id FROM resources FOR UPDATE;
  SET NEW.deduped_resource_id = last_id + 1;
END $$

DELIMITER ;
