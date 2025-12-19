from rule import Rule

rule = Rule("Max response time", "<=", 200)

print(rule.check(180))  # Expected output: True
print(rule.check(250))  # Expected output: False