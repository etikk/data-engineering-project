-- This script was generated by the ERD tool in pgAdmin 4.
-- Please log an issue at https://github.com/pgadmin-org/pgadmin4/issues/new/choose if you find any bugs, including reproduction steps.
BEGIN;


CREATE TABLE IF NOT EXISTS public."Article"
(
    "Article_Id" integer,
    "Title" text,
    "Comment" text,
    "Digital_Obj_Id" text,
    "Version_Id" integer,
    "Sub_Category" text,
    "Journal-ref" text,
    "Reference_Count" integer,
    PRIMARY KEY ("Article_Id")
);

CREATE TABLE IF NOT EXISTS public."Publications"
(
    "Update_Date" date,
    "Article_Id" integer,
    "Author_Id" integer
);

CREATE TABLE IF NOT EXISTS public."Version"
(
    "Version_Id" integer,
    "Creation_Date" date,
    "Version_Nbr" smallint,
    PRIMARY KEY ("Version_Id")
);

CREATE TABLE IF NOT EXISTS public."SubCategory"
(
    "SubCategory" text,
    "Main_Category" text,
    PRIMARY KEY ("SubCategory")
);

CREATE TABLE IF NOT EXISTS public."Author"
(
    "Author_Id" integer,
    "Full_Name" text,
    "Is_Submitter_Ind" text,
    PRIMARY KEY ("Author_Id")
);

CREATE TABLE IF NOT EXISTS public."Reference"
(
    "Journal_Ref" text,
    "Reference_Type" text,
    PRIMARY KEY ("Journal_Ref")
);

ALTER TABLE IF EXISTS public."Article"
    ADD CONSTRAINT "Ver" FOREIGN KEY ("Version_Id")
    REFERENCES public."Version" ("Version_Id") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Article"
    ADD CONSTRAINT "Cat" FOREIGN KEY ("Sub_Category")
    REFERENCES public."SubCategory" ("SubCategory") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Article"
    ADD CONSTRAINT "Ref" FOREIGN KEY ("Journal-ref")
    REFERENCES public."Reference" ("Journal_Ref") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Publications"
    ADD CONSTRAINT "Article" FOREIGN KEY ("Article_Id")
    REFERENCES public."Article" ("Article_Id") MATCH FULL
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Publications"
    ADD CONSTRAINT "Author" FOREIGN KEY ("Author_Id")
    REFERENCES public."Author" ("Author_Id") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;