SELECT SUM(CG1.v1),COUNT(*) FROM cycle
WHERE CG1.x BETWEEN 0 and 3750
AND   CG1.y BETWEEN 0 and 3750;
