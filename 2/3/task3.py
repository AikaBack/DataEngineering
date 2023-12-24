import json
import os
import msgpack

with open("products_52.json") as f:
    data = json.load(f)
   # print(data)

    products = dict()

    for item in data:
        if item['name'] in products:
            products[item['name']].append(item['price'])
        else:
            products[item['name']] = list()
            products[item['name']].append(item['price'])

    #print(products)

    result = list()

    for name, prices in products.items():
        sum_price = 0
        max_price = prices[0]
        min_prace = prices[0]
        size = len(prices)
        for price in prices:
            sum_price += price
            max_price = max(max_price, price)
            min_price = min(min_prace, price)

        result.append({
            "name": name,
            "max": max_price,
            "min": min_prace,
            "avr": sum_price / size
        })
#print(result)

with open("products_result.json", "w") as r_json:
    r_json.write(json.dumps(result))

with open("products_result.msgpack", "wb") as r_msgpack:
    r_msgpack.write(msgpack.dumps(result))

print(f"json = {os.path.getsize('products_result.json')}")
print(f"msgpack = {os.path.getsize('products_result.msgpack')}")

print(f"products_result.json Size: {os.path.getsize('products_result.json'):.2f} KB")
print(f"products_result.msgpack Size: {os.path.getsize('products_result.msgpack'):.2f} KB")