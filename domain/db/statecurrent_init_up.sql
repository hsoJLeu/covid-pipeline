create table statecurrent (
    id serial primary key,
    state                   varchar(10),
    positive	            integer,
    negative	            integer,
    recovered	            integer,
    death	                integer,
    hospitalized	        integer,
    totalTestResults	    integer,
    lastModified            varchar(100),
    hash	                varchar(255),
    UNIQUE (state)
)