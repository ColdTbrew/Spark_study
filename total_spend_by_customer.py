from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("TotalSpendByCustomer").setMaster("local[*]")
sc = SparkContext(conf=conf)

lines = sc.textFile('customer-orders.csv')

def parse_line(line):
    fields = line.split(',')
    customer_id = fields[0]
    order_amount = float(fields[2])
    return (customer_id, order_amount)

parsed_lines = lines.map(parse_line)
total_spend_by_customer = parsed_lines.reduceByKey(lambda x, y : x + y)

for customer, total_spend in total_spend_by_customer.collect():
    print(f"Customer {customer} spent a total of ${total_spend:.2f}")


print("-----------------")
sorted_total_spend = total_spend_by_customer.sortBy(lambda x: x[1], ascending=True)
for customer, total_spend in sorted_total_spend.collect():
    print(f"Customer {customer} spent a total of ${total_spend:.2f}")