alter table sections rename to classes;
alter table classes rename column class_section_id to class_id;
alter table accounts rename column class_section_id to class_id;
