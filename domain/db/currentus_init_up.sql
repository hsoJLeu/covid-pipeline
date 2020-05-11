create table current_us (
    id serial primary key,
    positive	            tinyint,
    negative	            tinyint,
    hospitalizedCurrently	tinyint,
    hospitalizedCumulative	tinyint,
    inIcuCurrently	        tinyint,
    inIcuCumulative	        tinyint,
    onVentilatorCurrently	tinyint,
    onVentilatorCumulative	tinyint,
    recovered	            tinyint,
    hash	                varchar(255),
    lastModified	        varchar(255),
    death	                tinyint,
    hospitalized	        tinyint,
    totalTestResults	    integer,
    notes	                varchar(255),
    UNIQUE(hash)
)