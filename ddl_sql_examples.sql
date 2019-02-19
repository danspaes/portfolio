create table academic_staff ( 
employee_no integer primary key,
first_name varchar(50),
last_name varchar(50),
Address varchar(50),
email varchar(50) CHECK (email LIKE '%@%.---'),
homepage varchar(50) CHECK (homepage LIKE '---.%.---'),
research_areas varchar(50)
);

create table project ( 
project_id integer primary key,
project_acronym varchar(10),
project_name varchar(50),
description varchar(50) null,
start_date date,
end_date date,
budget decimal(16,2),
fund_agency varchar(50)
);

create table participates ( 
employee_no integer,
project_id integer,
start_date date,
end_date date,
primary key (employee_no, project_id),
FOREIGN KEY (employee_no) REFERENCES academic_staff(employee_no),
FOREIGN KEY (project_id) REFERENCES project(project_id)
);

create table assistant ( 
advisor_id integer primary key,
thesis_title varchar(50),
thesis_description varchar(150)
);

create table professor ( 
professor_id integer null,
advisor_id integer null,
amt_courses integer,
tenure_date date null,
FOREIGN KEY (advisor_id) REFERENCES assistant(advisor_id)
);

create table course ( 
course_id integer primary key,
course_name varchar(50),
level varchar(10),
section_id integer,
course_prereq_id integer null
);

create table section ( 
section_id integer primary key,
course_id integer,
year_section integer,
homepage varchar(50),
professor_id integer,
FOREIGN KEY (course_id) REFERENCES course(course_id)
);

alter table course add foreign key (section_id) references section(section_id);
