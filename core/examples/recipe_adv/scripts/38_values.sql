# k1-adv2. Values whose store round trip could differ from a register: NaN / infinities, negative
# zero into REAL / INTEGER / NUMERIC / untyped, text-to-number edge forms, embedded NUL.
CREATE TABLE t(id INTEGER PRIMARY KEY, a REAL, b, c TEXT, d INTEGER, e NUMERIC)
INSERT INTO t(a, b) VALUES (1e308, 'x'), (-1e308, x'00'), (0.0, 3)
SELECT 1e308 * 10 - 1e308 * 10, typeof(1e308 * 10 - 1e308 * 10)
UPDATE t SET b = a * 10 - a * 10
SELECT id, b, typeof(b), quote(b) FROM t ORDER BY id
UPDATE t SET c = a * 10
SELECT id, c, typeof(c) FROM t ORDER BY id
UPDATE t SET d = -0.0 * a
SELECT id, d, typeof(d), quote(d) FROM t ORDER BY id
UPDATE t SET e = a * -0.0
SELECT id, e, typeof(e), quote(e), e || '' FROM t ORDER BY id
UPDATE t SET b = -0.0
SELECT id, b, typeof(b), quote(b), b || '', printf('%e', b) FROM t ORDER BY id
UPDATE t SET a = -0.0 WHERE id = 3
SELECT id, a, typeof(a), quote(a), a || '' FROM t ORDER BY id
UPDATE t SET c = 9223372036854775807 + 1.0
SELECT id, c, typeof(c) FROM t ORDER BY id
UPDATE t SET e = '9223372036854775808'
SELECT id, e, typeof(e) FROM t ORDER BY id
UPDATE t SET e = ' 12 '
SELECT id, e, typeof(e) FROM t ORDER BY id
UPDATE t SET e = '1e3'
SELECT id, e, typeof(e) FROM t ORDER BY id
UPDATE t SET d = '0x10'
SELECT id, d, typeof(d) FROM t ORDER BY id
UPDATE t SET d = 1.0e0
SELECT id, d, typeof(d) FROM t ORDER BY id
UPDATE t SET d = '  -7  '
SELECT id, d, typeof(d) FROM t ORDER BY id
UPDATE t SET e = 1.5e300 * 1e10
SELECT id, e, typeof(e), quote(e) FROM t ORDER BY id
UPDATE t SET b = char(0) || 'z'
SELECT id, length(b), hex(b), b = char(0) || 'z' FROM t ORDER BY id
UPDATE t SET c = 12345678901234567890
SELECT id, c, typeof(c) FROM t ORDER BY id
UPDATE t SET a = '1.0000000000000002'
SELECT id, a, typeof(a), quote(a) FROM t ORDER BY id
UPDATE t SET e = '0.1e1'
SELECT id, e, typeof(e) FROM t ORDER BY id
UPDATE t SET d = 2.5
SELECT id, d, typeof(d) FROM t ORDER BY id
