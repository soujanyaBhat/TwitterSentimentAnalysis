import json
from kafka import KafkaConsumer
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def main():

    consumer = KafkaConsumer('twitter')
    sentiment_id = SentimentIntensityAnalyzer()
    
    compound_number =0
    negative_sentiment_number = 0
    positive_sentiment_number = 0
    normal_sentiment_number = 0
    
    for message in consumer:
        text_data=json.loads(message.value)
        text=text_data["text"]
        sentiment_score=sentiment_id.polarity_scores(text_data["text"])

        x =max(sentiment_score, key=sentiment_score.get)
    
        if(x =='compound'):
            compound_number += 1
        elif (x == 'neg'):
            negative_sentiment_number += 1
            if (negative_sentiment_number<=5):
                print("Text:"+text+"- has negative sentiment")
        elif (x == 'neu'):
            normal_sentiment_number += 1
            if (normal_sentiment_number<=5):
               print("Text:"+text+"- has neutral sentiment")
        elif (x == 'pos'):
            positive_sentiment_number += 1
            if (positive_sentiment_number<=5):
                print("Text:"+text+"- has positive sentiment")
        if(negative_sentiment_number >=5 and normal_sentiment_number >=5 and positive_sentiment_number>=5):
            break

if __name__ == "__main__":
    main()