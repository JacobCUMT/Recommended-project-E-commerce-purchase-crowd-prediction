# Recommended-project-E-commerce-purchase-crowd-prediction
训练模型基本流程：

1. pyodps 读取数据集
2. dataset：读取数据、变换为 pytorch 格式，tensor
3. dataloader：组织数据，也可以数据变换
4. model：embedding、dense、relu
5. loss：celoss
6. optimizer：adam
7. 训练的流程：for
8. 保存 loss，tensorboard
9. 评估 auc
10. 保存模型

项目简介：
该项目为通过分析大量用户在平台上对商品的的点击、收藏、加购、购买的行为数据，结合商品信息，构建相关模型，通过用户行为特征、商品特征、交互特征等预测用户是否会购买相关商品，从而为商家提供个性化的推荐服务。
本项目基于阿里云进行开发，Maxcompute 提供计算服务，Dataworks 提供数据开发服务，PAI 提供模型训练服务。

数据集：
项目的数据集为天猫推荐数据集，共有用户 9774184 人，商品 8133507 个，品牌 89366 个，在 20130401-20131001 的时间段内，共产生了包含点击、收藏、加购、购买的 13 亿+条行为数据。
数据集地址：https://tianchi.aliyun.com/dataset/140281

基于 GBDT：

1. 数据开发：
   1. 数据
      数据仓储分层为数据运营层（ODS）、数据仓库层（DW）和数据服务层（ADS）和维表
      ODS 层的 user_item_beh_log 表存放原始的用户行为数据，记录了用户在某时刻对某商品的行为，
      DW 层存放经过清洗、处理、集成的结构化数据，结合 ODS 层和商品信息维表，分别建立了用户进行四种行为的详细信息 sql 表
      ADS 层存放经过高度聚合、计算和预处理的数据，从用户、品牌、用户品牌交互三个维度建表，基于 DW 层数据进一步开发特征
      维表存放开发过程中需要使用的维表数据，包括原有的商品信息，后期需要的 top500 品牌信息、用户编号信息等
   2. 样本
      user_pay_sample 表存放用户购买行为的样本，包含用户 id、商品 id、标签 (label=0 表示未购买，label=1 表示购买)
      user_pay_sample_feature_join 表存放样本和特征的融合，基于样本表和 ads 层各维度的表，用于模型训练
      补数据时，从 20130701-20130916，隔 7 天补一次。ADS 层数据用的是过去 90 天的数据，样本记录未来 7 天的购买行为。
   3. 评估
      user_pay_sample_eval 表存放模型评估过程中使用的评估样本
      user_pay_sample_feature_join_eval 表存放模型评估过程中使用的样本和特征的融合
      补数据时，补 20130923 的数据
      评估集中，为节省计算资源只用了 4 个品牌、300 万用户的样本
2. 模型训练：
   1. baseline：
      使用单个品牌的数据，单独训练一个模型，并进行预测
      模型训练使用的是 LightGBM 模型，模型训练过程中，使用的是 user_pay_sample_feature_join 表，
      模型评估指标为 AUC。
   2. 混合训练：
      为扩充样本，使用不同品牌的数据混合进行训练，并查看效果
   3. 所有品牌混合训练：
      为进一步扩充样本，使用所有品牌的数据混合进行训练，并查看效果
   4. 模型测试：
      模型测试使用的是 user_pay_sample_feature_join_eval 表，评估指标为 top-n 召回率。

基于 DNN：

1. 数据开发：

   2. 特征
      依旧使用 ADS 层的数据，与 GBDT 方案相比，增加用户行为序列特征，保存近 30 天用户对品牌及其二级类目的点击/收藏/购买行为序列特征(品牌为 top1w 的品牌)
   3. 样本
      为合理对比，与 GBDT 模型的样本开发相同
      创建好特征后，结合样本表，进行样本、特征融合。除原有的 user_id、brand_id、label、ADS 层的 7 个 feature 外，新增 target_brand_id, 和刚创建的 6 个品牌/二级类目 seq
      (注：要对 feature 进行特征拼接和特征离散化)
   4. 训练
      特征序列化，创建 user_pay_sample_feature_seq 表，给所有特征编号
      然后创建 User_pay_sample_feature_join_dnn_seq 表，将所有特征转换为刚才的编号
      创建 suffle 表，对训练集进行乱序处理(最终用 shuffle 表训练)
   5. 评估
      创建同上的特征表，注意编号要和训练集一致！

2. 模型训练：

   1. 模型训练
      模型训练使用的是 DNN 模型，模型训练过程中，使用的是 user_pay_sample_feature_join_dnn_seq_shuffle 表，模型评估指标为 AUC。
   2. 模型测试
      使用 user_pay_sample_feature_join_eval_dnn_seq 表，评估指标仍然为 top-n 召回率。

   优化点：Dataset 加载数据时，不要先把 900w 条数据 split 后的 tensor 加载到内存，而是先把原始的 numpy 格式加载进内存，在 getitem 时再转换为 tensor，这样可以节省内存。

   加入 focal_loss 损失函数：
   在 dnn 的基础上，将损失函数改为 focal_loss
   focal loss 的作用是在样本不均衡时，将难分类的样本的 loss 放大，容易分类的样本的 loss 缩小，从而使得模型更加关注难分类的样本。
   进行改动后，发现没有效果。猜测原因可能为：相比于原论文中的目标检测场景，该项目场景中容易样本比例较低，导致困难样本的 loss 不会得到更高的权重。

基于 DIN：

1. 数据开发：
   数据、样本、特征、loss、评估方案皆与 DNN 方案一致，

2. 模型训练：
   与 DNN 方案相比，增加用户行为与目标品牌间的注意力，从而让模型通过 attention 自动学习品牌之间的关联关系，提升模型效果。
   seq_collate_fn 函数计算序列的 mask 信息：
   dataloader 中，为处理变长序列数据，需要对其进行 padding 以对其长度
   用 mask 区分实际数据和 padding 数据

   din_model 中，写 LocalActivationUnit 类，计算“用户对品牌行为”与“目标品牌 id”之间的 interest，在模型中把对应行为特征换为对应 interest(保持维度变换一致，不用修改模型)

   把测试直接融入训练过程，验证时用测试数据直接得到结果即可

基于 MOE：

1. 数据开发：
   数据、样本、特征、loss、评估方案皆与 DNN 方案一致，

2. 模型训练：
   与 DIN 方案相比，增加专家网络和门控网络，门控网络优化权重分布，专家网络优化自身性能，两者协作使 MOE 模型具备强大的动态适应能力。
   把模型的前向传播直接写进 expert 类中
