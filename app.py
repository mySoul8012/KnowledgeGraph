import requests
import json
from neo4j import GraphDatabase

# -----------------------------
# 接口地址
# -----------------------------
PREDICT_URL = "http://localhost:8088/predict"
INFER_URL = "http://127.0.0.1:5000/infer"

HEADERS = {
    "Content-Type": "application/json"
}

# -----------------------------
# Neo4j Aura连接配置
# -----------------------------
NEO4J_URI = "neo4j+s://db4459f8.databases.neo4j.io"
NEO4J_USER = "db4459f8"
NEO4J_PASSWORD = "Yy2FTjhntuTTU48N54VJZYTe1jms0HaEALNlMyzfygk"
NEO4J_DATABASE = "db4459f8"

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

# -----------------------------
# 文本纠错接口
# -----------------------------
def call_predict(text):

    payload = {
        "texts": [text]
    }

    resp = requests.post(PREDICT_URL, headers=HEADERS, json=payload)
    resp.raise_for_status()

    result = resp.json()

    print("\n===== predict接口返回 =====")
    print(json.dumps(result, indent=4, ensure_ascii=False))

    if "texts" in result:
        return result["texts"][0]

    return text


# -----------------------------
# 实体抽取
# -----------------------------
def extract_entities(text):

    payload = {
        "text": text,
        "schema": ["人名", "机构", "地点", "时间"]
    }

    resp = requests.post(INFER_URL, headers=HEADERS, json=payload)
    resp.raise_for_status()

    result = resp.json()

    print("\n===== 实体抽取结果 =====")
    print(json.dumps(result, indent=4, ensure_ascii=False))

    return result


# -----------------------------
# 关系抽取
# -----------------------------
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

    print("\n===== 关系抽取结果 =====")
    print(json.dumps(result, indent=4, ensure_ascii=False))

    return result


# -----------------------------
# 写入Neo4j
# -----------------------------
def save_to_neo4j(data):

    with driver.session(database=NEO4J_DATABASE) as session:

        text = data["text"]

        entities = data["entities"]["result"][0]
        relations = data["relations"]["result"][0]

        event_text = None

        # 创建比赛事件节点
        if "比赛事件" in relations:

            event_text = relations["比赛事件"][0]["text"]

            session.run(
                """
                MERGE (e:比赛事件 {name:$name})
                SET e.source_text=$source
                """,
                name=event_text,
                source=text
            )

        # 时间节点
        if "时间" in entities:

            for t in entities["时间"]:

                time_text = t["text"]

                session.run(
                    """
                    MERGE (t:时间 {name:$name})
                    """,
                    name=time_text
                )

                if event_text:
                    session.run(
                        """
                        MATCH (e:比赛事件 {name:$event})
                        MATCH (t:时间 {name:$time})
                        MERGE (e)-[:发生时间]->(t)
                        """,
                        event=event_text,
                        time=time_text
                    )

        # 地点节点
        if "地点" in entities:

            for loc in entities["地点"]:

                loc_text = loc["text"]

                session.run(
                    """
                    MERGE (l:地点 {name:$name})
                    """,
                    name=loc_text
                )

                if event_text:
                    session.run(
                        """
                        MATCH (e:比赛事件 {name:$event})
                        MATCH (l:地点 {name:$loc})
                        MERGE (e)-[:发生地点]->(l)
                        """,
                        event=event_text,
                        loc=loc_text
                    )

    print("\n数据已成功写入 Neo4j Aura")


# -----------------------------
# 主流程
# -----------------------------
def main():

    text = "2022年北京马拉松在北京市举办，吸引了大量跑者参加。"

    print("原始文本:", text)

    # Step1 文本纠错
    normalized_text = call_predict(text)

    print("\n纠错后的文本:", normalized_text)

    # Step2 实体抽取
    entity_result = extract_entities(normalized_text)

    # Step3 关系抽取
    relation_result = extract_relations(normalized_text)

    print("\n===== 最终汇总 =====")

    final_result = {
        "text": normalized_text,
        "entities": entity_result,
        "relations": relation_result
    }

    print(json.dumps(final_result, indent=4, ensure_ascii=False))

    # Step4 写入Neo4j
    save_to_neo4j(final_result)


if __name__ == "__main__":
    main()