INSERT INTO region VALUES
  (0, 'AFRICA', 'special Tiresias'),
  (1, 'AMERICA', 'hs use ironic, even'),
  (2, 'ASIA', 'ges. thinly even pinto beans'),
  (3, 'EUROPE', 'ly final courts'),
  (4, 'MIDDLE EAST', 'uickly special accounts');

INSERT INTO nation VALUES
  (0,  'ALGERIA',       0, 'furiously regular'),
  (1,  'ARGENTINA',     1, 'al foxes promise'),
  (2,  'BRAZIL',        1, 'y alongside of'),
  (3,  'CANADA',        1, 'eas hang ironic'),
  (5,  'ETHIOPIA',      0, 'ven packages wake'),
  (6,  'FRANCE',        3, 'refully final requests'),
  (7,  'GERMANY',       3, 'l platelets. regular'),
  (8,  'INDIA',         2, 'ss excuses cajole'),
  (9,  'INDONESIA',     2, 'slyly express asymptotes'),
  (10, 'IRAN',          4, 'efully alongside of'),
  (12, 'JAPAN',         2, 'ously. final, express'),
  (15, 'MOROCCO',       0, 'rns. blithely bold'),
  (17, 'PERU',          1, 'platelets. blithely pending'),
  (24, 'UNITED STATES', 1, 'y final packages');

INSERT INTO supplier VALUES
  (1, 'Supplier#000000001', 'N kD4on9OM Ipw3', 17, '27-918-335-1736', 5755.94, 'each slyly above'),
  (2, 'Supplier#000000002', '89eJ5ksX3I',       5, '15-679-861-2259', 4032.68, 'slyly bold instructions'),
  (3, 'Supplier#000000003', 'q1,G3Pj6OjIuUY',   1, '11-383-516-1199', 4192.40, 'blithely silent requests'),
  (4, 'Supplier#000000004', 'Bk7ah4CK8SYQTep',  15, '25-843-787-7479', 4641.08, 'riously stealthy');

INSERT INTO part VALUES
  (1,  'goldenrod lace spring',   'Manufacturer#1', 'Brand#13', 'PROMO BURNISHED COPPER',   7, 'JUMBO PKG',  901.00, 'ly. slyly ironi'),
  (2,  'blush thistle blue',      'Manufacturer#1', 'Brand#13', 'LARGE BRUSHED BRASS',      1, 'LG CASE',    902.00, 'lar accounts acco'),
  (3,  'spring green yellow',     'Manufacturer#4', 'Brand#42', 'STANDARD POLISHED BRASS',  21, 'WRAP CASE',  903.00, 'egular deposits'),
  (4,  'cornflower chocolate',    'Manufacturer#3', 'Brand#34', 'SMALL PLATED BRASS',       14, 'MED DRUM',   904.00, 'p]ironic foxes'),
  (5,  'forest brown coral',      'Manufacturer#3', 'Brand#32', 'STANDARD POLISHED TIN',    15, 'SM PKG',     905.00, 'wake carefully');

INSERT INTO partsupp VALUES
  (1, 1, 3325, 771.64, 'requests after the carefully ironic ideas'),
  (1, 2, 8076, 993.49, 'ven ideas. quickly even packages'),
  (2, 1, 8895, 378.49, 'nsing foxes. quickly final'),
  (2, 3, 4651, 920.92, 'e blithely along the ironic'),
  (3, 2, 3012, 530.82, 'special pinto beans hang'),
  (3, 4, 4124, 890.22, 'ly regular platelets'),
  (4, 1, 1244, 444.82, 'al, ironic ideas nod silently'),
  (4, 3, 6492, 555.82, 'fter the carefully pending'),
  (5, 2, 2723, 691.03, 'olites. blithely ironic'),
  (5, 4, 7601, 231.67, 'the stealthy, regular');

INSERT INTO customer VALUES
  (1, 'Customer#000000001', 'IVhzIApeRb',          15, '25-989-741-2988', 711.56,  'BUILDING',   'to the even, regular'),
  (2, 'Customer#000000002', 'XSTf4,NCwDVaW',       1,  '11-719-748-3364', 121.65,  'AUTOMOBILE', 'l accounts. blithely'),
  (3, 'Customer#000000003', 'MG9kdTD2WBHm',        1,  '11-719-748-3364', 7498.12, 'AUTOMOBILE', 'deposits eat slyly ironic'),
  (4, 'Customer#000000004', 'XxVSJsLAGtn',         3,  '13-137-193-2709', 2866.83, 'MACHINERY',  'requests. final, regular'),
  (5, 'Customer#000000005', 'KvpyuHCplrB84WgAi',   3,  '13-750-942-6364', 794.47,  'HOUSEHOLD',  'n accounts was');

INSERT INTO orders VALUES
  (1, 1, 'O', 173665.47, DATE '1996-01-02', '5-LOW',        'Clerk#000000951', 0, 'nstructions sleep furiously'),
  (2, 2, 'O', 46929.18,  DATE '1996-12-01', '1-URGENT',     'Clerk#000000880', 0, 'foxes. pending accounts'),
  (3, 3, 'F', 193846.25, DATE '1993-10-14', '5-LOW',        'Clerk#000000955', 0, 'sly final accounts boost'),
  (4, 4, 'O', 32151.78,  DATE '1995-10-11', '5-LOW',        'Clerk#000000124', 0, 'sits. slyly regular warthogs'),
  (5, 5, 'F', 144659.20, DATE '1994-07-30', '5-LOW',        'Clerk#000000925', 0, 'quickly. bold deposits sleep'),
  (6, 1, 'F', 58749.59,  DATE '1992-02-21', '4-NOT SPECIFIED', 'Clerk#000000058', 0, 'ggle. special, final'),
  (7, 2, 'O', 252004.18, DATE '1996-01-10', '2-HIGH',       'Clerk#000000470', 0, 'ly special requests');

INSERT INTO lineitem VALUES
  (1, 1, 1, 1, 17, 21168.23, 0.04, 0.02, 'N', 'O', DATE '1996-03-13', DATE '1996-02-12', DATE '1996-03-22', 'DELIVER IN PERSON',  'TRUCK',     'egular courts above'),
  (1, 2, 1, 2, 36, 34850.16, 0.09, 0.06, 'N', 'O', DATE '1996-04-12', DATE '1996-02-28', DATE '1996-04-20', 'TAKE BACK RETURN',   'MAIL',      'ly final dependencies'),
  (2, 3, 2, 1, 38, 44694.46, 0.00, 0.05, 'N', 'O', DATE '1997-01-28', DATE '1997-01-14', DATE '1997-02-02', 'TAKE BACK RETURN',   'RAIL',      'ven requests. deposits breach'),
  (3, 1, 1, 1, 45, 54058.05, 0.06, 0.00, 'R', 'F', DATE '1994-02-02', DATE '1994-01-04', DATE '1994-02-23', 'NONE',               'AIR',       'ongside of the furiously brave'),
  (3, 2, 2, 2, 49, 46796.47, 0.10, 0.00, 'R', 'F', DATE '1993-11-09', DATE '1993-12-20', DATE '1993-11-24', 'TAKE BACK RETURN',   'RAIL',      'unusual accounts. eve'),
  (4, 1, 1, 1, 30, 37260.00, 0.03, 0.08, 'N', 'O', DATE '1996-01-10', DATE '1995-12-14', DATE '1996-01-18', 'DELIVER IN PERSON',  'REG AIR',   'tions. blithely regular'),
  (5, 3, 2, 1, 15, 17428.35, 0.02, 0.04, 'R', 'F', DATE '1994-10-31', DATE '1994-08-31', DATE '1994-11-20', 'NONE',               'AIR',       'ts wake furiously'),
  (5, 4, 3, 2, 26, 28348.06, 0.07, 0.08, 'R', 'F', DATE '1994-08-08', DATE '1994-10-07', DATE '1994-08-26', 'DELIVER IN PERSON',  'AIR',       'he accounts. fluffily'),
  (6, 5, 4, 1, 37, 36424.49, 0.08, 0.03, 'A', 'F', DATE '1992-04-27', DATE '1992-05-15', DATE '1992-05-02', 'TAKE BACK RETURN',   'TRUCK',     'p ironic, regular deposits'),
  (7, 1, 1, 1, 12, 15012.00, 0.07, 0.03, 'N', 'O', DATE '1996-02-01', DATE '1996-03-02', DATE '1996-02-19', 'COLLECT COD',        'SHIP',      'al foxes promise slyly');
