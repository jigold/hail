DELIMITER $$

DROP TRIGGER IF EXISTS resources_before_insert $$
CREATE TRIGGER resources_before_insert BEFORE INSERT ON resources
FOR EACH ROW
BEGIN
  DECLARE last_id INT;

  SELECT MAX(resource_id) INTO last_id FROM resources;

  IF last_id IS NULL THEN
    SET NEW.deduped_resource_id = 1
  ELSE
    SET NEW.deduped_resource_id = last_id + 1
  END IF;
END $$

DELIMITER ;
