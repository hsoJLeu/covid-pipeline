create table daily (
	id serial primary key,
	date_id int,
	UNIQUE (date_id)

);

create table statehistorical (
	date			  		 int ,
	state                    text,
	positive                 int,
	negative                 int,
	pending                  int,
	hospitalizedCurrently    int,
	hospitalizedCumulative   int,
	inIcuCurrently           int,
	inIcuCumulative          int,
	onVentilatorCurrently    int,
	onVentilatorCumulative   int,
	recovered                int,
	death					 int,
	hospitalized             int,
	totalTestResults         int,
	hospitalizedIncrease     int,
	deathIncrease            int,
	negativeIncrease         int,
	positiveIncrease         int,
	totalTestResultsIncrease int,
	hash					 varchar(255),
	UNIQUE (hash)
);
