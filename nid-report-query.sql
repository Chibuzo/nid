SELECT pp.FIRST_NAME, pp.LAST_NAME, pp.MIDDLE_NAMES, pp.SEX GENDER, pp.TITLE, pp.NATIONAL_IDENTIFIER NID, pp.EMPLOYEE_NUMBER, p.name POSITION,
    g.NAME GRADE, PAYROLL_NAME, org.name DEPARTMENT, org.ATTRIBUTE5 MINISTRY,
    CASE WHEN pp.ATTRIBUTE10 = 'verified' THEN 'Yes' ELSE 'No' END AS NID_VERIFIED,
    CASE WHEN pp.ATTRIBUTE11 = 'dead' THEN 'Yes' ELSE 'no' END AS DEAD
FROM HR.PER_ALL_PEOPLE_F pp
    LEFT JOIN HR.PER_ALL_ASSIGNMENTS_F a  USING(PERSON_ID)
    JOIN hr_all_organization_units org ON a.organization_id = org.organization_id
    LEFT JOIN HR.HR_ALL_POSITIONS_F p      using (position_id)
    LEFT OUTER JOIN HR.PER_GRADES g      USING(GRADE_ID)
    LEFT JOIN HR.PAY_ALL_PAYROLLS_F pr ON pr.PAYROLL_ID = a.PAYROLL_ID
WHERE pp.ATTRIBUTE10 = 'verified'

SELECT pp.FIRST_NAME, pp.LAST_NAME, pp.MIDDLE_NAMES, pp.SEX GENDER, pp.TITLE, pp.NATIONAL_IDENTIFIER NID, pp.EMPLOYEE_NUMBER, p.name POSITION,
    g.NAME GRADE, PAYROLL_NAME, org.name DEPARTMENT, org.ATTRIBUTE5 MINISTRY,
    CASE WHEN pp.ATTRIBUTE10 = 'verified' THEN 'Yes' ELSE 'No' END AS NID_VERIFIED,
    CASE WHEN pp.ATTRIBUTE11 = 'dead' THEN 'Yes' ELSE 'no' END AS DEAD
FROM HR.PER_ALL_PEOPLE_F pp
    LEFT JOIN HR.PER_ALL_ASSIGNMENTS_F a      USING(PERSON_ID)
    JOIN hr_all_organization_units org ON a.organization_id = org.organization_id
    LEFT JOIN HR.HR_ALL_POSITIONS_F p      using (position_id)
    LEFT OUTER JOIN HR.PER_GRADES g      USING(GRADE_ID)
    LEFT JOIN HR.PAY_ALL_PAYROLLS_F pr ON pr.PAYROLL_ID = a.PAYROLL_ID
WHERE pp.ATTRIBUTE10 <> 'verified'

SELECT pp.FIRST_NAME, pp.LAST_NAME, pp.MIDDLE_NAMES, pp.SEX GENDER, pp.TITLE, pp.NATIONAL_IDENTIFIER NID, pp.EMPLOYEE_NUMBER, p.name POSITION,
    g.NAME GRADE, DATE_OF_DEATH, PAYROLL_NAME, org.name DEPARTMENT, org.ATTRIBUTE5 MINISTRY,
    CASE WHEN pp.ATTRIBUTE10 = 'verified' THEN 'Yes' ELSE 'No' END AS NID_VERIFIED,
    CASE WHEN pp.ATTRIBUTE11 = 'dead' THEN 'Yes' ELSE 'no' END AS DEAD
FROM HR.PER_ALL_PEOPLE_F pp
    LEFT JOIN HR.PER_ALL_ASSIGNMENTS_F a       USING(PERSON_ID)
    JOIN hr_all_organization_units org ON a.organization_id = org.organization_id
    LEFT JOIN HR.HR_ALL_POSITIONS_F p       using (position_id)
    LEFT OUTER JOIN HR.PER_GRADES g       USING(GRADE_ID)
    LEFT JOIN HR.PAY_ALL_PAYROLLS_F pr ON pr.PAYROLL_ID = a.PAYROLL_ID
WHERE pp.ATTRIBUTE11 = 'dead'

