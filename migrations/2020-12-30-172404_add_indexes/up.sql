create index filesystem_inode_index
	on filesystem (inode);

create index filesystem_name_parent_index
	on filesystem (parent_inode, name);

create index filesystem_parent_index
	on filesystem (parent_id, parent_inode);