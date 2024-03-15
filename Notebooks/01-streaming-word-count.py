class WordCountBase:
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def read_data(self, mode: str) -> DataFrame:
        lines = (
            spark.readStream.format("text")
            if mode == "stream"
            else spark.read.format("text")
        )
        lines = lines.option("lineSep", ".").load(f"{self.base_data_dir}/data/text")
        return lines.select(explode(split(lines.value, " ")).alias("word"))

    def get_quality_data(self, rawDF: DataFrame) -> DataFrame:
        return rawDF.select(lower(trim(rawDF.word)).alias("word")).where(
            "word is not null and word rlike '[a-z]'"
        )

    def get_word_count(self, qualityDF: DataFrame) -> DataFrame:
        return qualityDF.groupBy("word").count()


class BatchWordCount(WordCountBase):
    def overwrite_word_count(self, wordCountDF: DataFrame):
        wordCountDF.write.format("delta").mode("overwrite").saveAsTable(
            "word_count_table"
        )

    def word_count(self):
        print("\tExecuting Word Count...", end="")
        rawDF = self.read_data("batch")
        qualityDF = self.get_quality_data(rawDF)
        resultDF = self.get_word_count(qualityDF)
        self.overwrite_word_count(resultDF)
        print("Done")


class StreamWordCount(WordCountBase):
    def overwrite_word_count(self, wordCountDF: DataFrame):
        return (
            wordCountDF.writeStream.format("delta")
            .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/word_count")
            .outputMode("complete")
            .toTable("word_count_table")
        )

    def word_count(self):
        print("\tStarting Word Count Stream...", end="")
        rawDF = self.read_data("stream")
        qualityDF = self.get_quality_data(rawDF)
        resultDF = self.get_word_count(qualityDF)
        sQuery = self.overwrite_word_count(resultDF)
        print("Done")
        return sQuery
