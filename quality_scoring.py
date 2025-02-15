import pandas as pd


df = pd.read_csv("titanic.csv")
print(df.head())

# วิธีการเรียกดูค่าใน Column
# df["Survived"]
# df.Survived

df.info()

name_not_null = df.Name.notnull()
dq_Name = name_not_null.sum() / len(df)
print(f"Data Quality of Name: {dq_Name}")


pclass_not_null = df.Pclass.notnull()
dq_Pclass = pclass_not_null.sum() / len(df)
print(f"Data Quality of Pclass: {dq_Pclass}")


survived_not_null = df.Survived.notnull()
dq_Survived = survived_not_null.sum() / len(df)
print(f"Data Quality of Survived: {dq_Survived}")


Passenger_id_not_null = df.PassengerId.notnull()
dq_Passenger_id = Passenger_id_not_null.sum() / len(df)
print(f"Data Quality of PassengerId: {dq_Passenger_id}")


age_not_null = df.Age.notnull()
dq_age = age_not_null.sum() / len(df)
print(f"Data Quality of Age: {dq_age}")

cabin_not_null = df.Cabin.notnull()
dq_cabin = cabin_not_null.sum() / len(df)
print(f"Data Quality of Cabin: {dq_cabin}")

embarked_not_null = df.Embarked.notnull()
dq_embarked = embarked_not_null.sum() / len(df)
print(f"Data Quality of Embarked: {dq_embarked}")

print(f"Completeness: {(dq_Name+dq_Pclass+dq_Survived+dq_Passenger_id+dq_age + dq_cabin + dq_embarked) / 7}")



