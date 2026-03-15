import requests
import json
from neo4j import GraphDatabase

# --------------------------------
# API 地址
# --------------------------------
PREDICT_URL = "http://localhost:8088/predict"
INFER_URL = "http://127.0.0.1:5000/infer"

HEADERS = {
    "Content-Type": "application/json"
}

# --------------------------------
# Neo4j Aura 连接配置
# --------------------------------
NEO4J_URI = "neo4j+s://db4459f8.databases.neo4j.io"
NEO4J_USER = "db4459f8"
NEO4J_PASSWORD = "Yy2FTjhntuTTU48N54VJZYTe1jms0HaEALNlMyzfygk"
NEO4J_DATABASE = "db4459f8"

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)


# --------------------------------
# 文本纠错
# --------------------------------
def call_predict(text):

    payload = {"texts": [text]}

    resp = requests.post(PREDICT_URL, headers=HEADERS, json=payload)
    resp.raise_for_status()

    result = resp.json()

    print("\n===== predict结果 =====")
    print(json.dumps(result, indent=4, ensure_ascii=False))

    if "texts" in result:
        return result["texts"][0]

    return text


# --------------------------------
# 实体抽取
# --------------------------------
def extract_entities(text):

    payload = {
        "text": text,
        "schema": ["人名", "机构", "地点", "时间"]
    }

    resp = requests.post(INFER_URL, headers=HEADERS, json=payload)
    resp.raise_for_status()

    result = resp.json()

    print("\n===== 实体抽取 =====")
    print(json.dumps(result, indent=4, ensure_ascii=False))

    return result


# --------------------------------
# 关系抽取
# --------------------------------
def extract_relations(text):

    payload = {
        "text": text,
        "schema": {
            "比赛事件": [
                "赛事名称",
                "时间",
                "地点"
            ]
        }
    }

    resp = requests.post(INFER_URL, headers=HEADERS, json=payload)
    resp.raise_for_status()

    result = resp.json()

    print("\n===== 关系抽取 =====")
    print(json.dumps(result, indent=4, ensure_ascii=False))

    return result


# --------------------------------
# 查询实体
# --------------------------------
def query_entities(entity_result):

    print("\n===== 查询实体 =====")

    entities = []

    results = entity_result.get("result", [])

    for block in results:

        for label, items in block.items():

            for item in items:

                name = item.get("text")

                if name:
                    entities.append(name)

    with driver.session(database=NEO4J_DATABASE) as session:

        for name in entities:

            cypher = """
            MATCH (n {name:$name})
            RETURN labels(n) AS labels, n.name AS name
            """

            result = session.run(cypher, name=name)

            for record in result:

                print({
                    "labels": record["labels"],
                    "name": record["name"]
                })


# --------------------------------
# 查询关系
# --------------------------------
def query_relations(relation_result):

    print("\n===== 查询关系 =====")

    results = relation_result.get("result", [])

    with driver.session(database=NEO4J_DATABASE) as session:

        for block in results:

            for event_type, items in block.items():

                for item in items:

                    event_name = item.get("text")

                    if not event_name:
                        continue

                    cypher = """
                    MATCH (a {name:$name})-[r]->(b)
                    RETURN labels(a) AS from_labels,
                           a.name AS from_name,
                           type(r) AS relation,
                           labels(b) AS to_labels,
                           b.name AS to_name
                    """

                    result = session.run(cypher, name=event_name)

                    for record in result:

                        row = {
                            "from": {
                                "labels": record["from_labels"],
                                "name": record["from_name"]
                            },
                            "relation": record["relation"],
                            "to": {
                                "labels": record["to_labels"],
                                "name": record["to_name"]
                            }
                        }

                        print(row)


# --------------------------------
# 主流程
# --------------------------------
def main():

    text = "2022年北京马拉松在北京市举办，吸引了大量跑者参加。"

    print("原始文本:", text)

    # Step1 文本纠错
    normalized_text = call_predict(text)

    print("\n纠错后文本:", normalized_text)

    # Step2 实体抽取
    entity_result = extract_entities(normalized_text)

    # Step3 关系抽取
    relation_result = extract_relations(normalized_text)

    print("\n===== 最终结果 =====")

    final_result = {
        "text": normalized_text,
        "entities": entity_result,
        "relations": relation_result
    }

    print(json.dumps(final_result, indent=4, ensure_ascii=False))

    # Step4 查询实体
    query_entities(entity_result)

    # Step5 查询关系
    query_relations(relation_result)


if __name__ == "__main__":
    main()