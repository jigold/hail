ALTER TABLE `attempts` ADD CONSTRAINT fk_instance_name
  FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE;

