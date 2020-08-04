# ES-Apriori based Stock rule Miner
This program was mainly a coursework design project

Using ES-Apriori to generate frequent Ln items, using support threshold and confidence threshold for rule generation

## Bitmap incorperated improvement
Using ES-Apriori Algorithm conbined with the idea Bitmap encoding, which I conducted several experiment, and the results shows that by using a bitmap encoding for items, the variance of ES-Apriori algorithm was significantly reduced. This improvement could be attributed to the avoidance of combination term break out problem by bitmap coded ES-Apriori

## Experiment Pictures

![bitmap_size](https://github.com/elenathFGS/ES_Apriori_stocker/tree/master/Images/bitmap_size.png)

![stock_rules](https://github.com/elenathFGS/ES_Apriori_stocker/tree/master/Images/stock_rules.png)