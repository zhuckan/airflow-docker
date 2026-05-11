{\rtf1\ansi\ansicpg1251\cocoartf2867
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\froman\fcharset0 Times-Roman;}
{\colortbl;\red255\green255\blue255;\red13\green14\blue17;\red255\green255\blue255;\red43\green71\blue253;
\red230\green234\blue239;}
{\*\expandedcolortbl;;\cssrgb\c5882\c6667\c8235;\cssrgb\c100000\c100000\c100000;\cssrgb\c22353\c39216\c99608;
\cssrgb\c92157\c93333\c94902;}
{\*\listtable{\list\listtemplateid1\listhybrid{\listlevel\levelnfc0\levelnfcn0\leveljc0\leveljcn0\levelfollow0\levelstartat2\levelspace360\levelindent0{\*\levelmarker \{decimal\}}{\leveltext\leveltemplateid1\'01\'00;}{\levelnumbers\'01;}\fi-360\li720\lin720 }{\listname ;}\listid1}}
{\*\listoverridetable{\listoverride\listid1\listoverridecount0\ls1}}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs28 \cf0 Airflow ML Pipeline: \uc0\u1087 \u1088 \u1086 \u1075 \u1085 \u1086 \u1079  \u1094 \u1077 \u1085  \u1072 \u1074 \u1090 \u1086 \u1084 \u1086 \u1073 \u1080 \u1083 \u1077 \u1081 \
\
\uc0\u1040 \u1074 \u1090 \u1086 \u1084 \u1072 \u1090 \u1080 \u1079 \u1072 \u1094 \u1080 \u1103  \u1086 \u1073 \u1091 \u1095 \u1077 \u1085 \u1080 \u1103  \u1080  \u1080 \u1085 \u1092 \u1077 \u1088 \u1077 \u1085 \u1089 \u1072  \u1084 \u1086 \u1076 \u1077 \u1083 \u1080  \u1084 \u1072 \u1096 \u1080 \u1085 \u1085 \u1086 \u1075 \u1086  \u1086 \u1073 \u1091 \u1095 \u1077 \u1085 \u1080 \u1103  \u1089  \u1087 \u1086 \u1084 \u1086 \u1097 \u1100 \u1102  Apache Airflow.\
\
\uc0\u1063 \u1090 \u1086  \u1074 \u1085 \u1091 \u1090 \u1088 \u1080 \
\
*   pipeline \'97 \uc0\u1079 \u1072 \u1075 \u1088 \u1091 \u1079 \u1082 \u1072  \u1076 \u1072 \u1085 \u1085 \u1099 \u1093 , \u1087 \u1088 \u1077 \u1076 \u1086 \u1073 \u1088 \u1072 \u1073 \u1086 \u1090 \u1082 \u1072 , \u1086 \u1073 \u1091 \u1095 \u1077 \u1085 \u1080 \u1077  \u1085 \u1077 \u1089 \u1082 \u1086 \u1083 \u1100 \u1082 \u1080 \u1093  \u1084 \u1086 \u1076 \u1077 \u1083 \u1077 \u1081  (LogisticRegression, RandomForest, SVC), \u1074 \u1099 \u1073 \u1086 \u1088  \u1083 \u1091 \u1095 \u1096 \u1077 \u1081  \u1087 \u1086  \u1082 \u1088 \u1086 \u1089 \u1089 -\u1074 \u1072 \u1083 \u1080 \u1076 \u1072 \u1094 \u1080 \u1080  \u1080  \u1089 \u1086 \u1093 \u1088 \u1072 \u1085 \u1077 \u1085 \u1080 \u1077 .\
*   predict \'97 \uc0\u1079 \u1072 \u1075 \u1088 \u1091 \u1079 \u1082 \u1072  \u1089 \u1086 \u1093 \u1088 \u1072 \u1085 \u1105 \u1085 \u1085 \u1086 \u1081  \u1084 \u1086 \u1076 \u1077 \u1083 \u1080  \u1080  \u1087 \u1088 \u1077 \u1076 \u1089 \u1082 \u1072 \u1079 \u1072 \u1085 \u1080 \u1077  \u1076 \u1083 \u1103  JSON-\u1092 \u1072 \u1081 \u1083 \u1086 \u1074  \u1080 \u1079  data/test.\
*   DAG \uc0\u1085 \u1072 \u1089 \u1090 \u1088 \u1086 \u1077 \u1085  \u1085 \u1072  \u1077 \u1078 \u1077 \u1076 \u1085 \u1077 \u1074 \u1085 \u1099 \u1081  \u1079 \u1072 \u1087 \u1091 \u1089 \u1082  \u1074   15:00.\
\
\uc0\u1057 \u1090 \u1077 \u1082 :\
Python \'b7 Airflow 3.2.1 \'b7 Docker Compose \'b7 Scikit-learn \'b7 Pandas \'b7 Dill \'b7 PostgreSQL \'b7 Redis\
\
\uc0\u1041 \u1099 \u1089 \u1090 \u1088 \u1099 \u1081  \u1089 \u1090 \u1072 \u1088 \u1090 :\
\
1.  \uc0\u1050 \u1083 \u1086 \u1085 \u1080 \u1088 \u1091 \u1081 \u1090 \u1077  \u1088 \u1077 \u1087 \u1086 \u1079 \u1080 \u1090 \u1086 \u1088 \u1080 \u1081 :\
bash\
git clone <URL>\
cd airflow-docker\cf2 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 \
\
2. \cf2 \cb3 \strokec2 \uc0\u1053 \u1072 \u1089 \u1090 \u1088 \u1086 \u1081 \u1090 \u1077  \u1087 \u1077 \u1088 \u1077 \u1084 \u1077 \u1085 \u1085 \u1099 \u1077  \u1086 \u1082 \u1088 \u1091 \u1078 \u1077 \u1085 \u1080 \u1103 :\
cp .env.example .env\
\uc0\u1086 \u1090 \u1088 \u1077 \u1076 \u1072 \u1082 \u1090 \u1080 \u1088 \u1091 \u1081 \u1090 \u1077  .env, \u1074 \u1089 \u1090 \u1072 \u1074 \u1080 \u1074  \u1074 \u1072 \u1096 \u1080  \u1088 \u1077 \u1072 \u1083 \u1100 \u1085 \u1099 \u1077  \u1082 \u1083 \u1102 \u1095 \u1080 \
\
3. \uc0\u1055 \u1086 \u1076 \u1085 \u1080 \u1084 \u1080 \u1090 \u1077  \u1089 \u1077 \u1088 \u1074 \u1080 \u1089 \u1099 :\
docker compose up -d\
\pard\tx220\tx720\pardeftab720\li720\fi-720\partightenfactor0
\ls1\ilvl0\cf2 \cb3 \kerning1\expnd0\expndtw0 \outl0\strokewidth0 \
{\listtext	3	}4. \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 \uc0\u1054 \u1090 \u1082 \u1088 \u1086 \u1081 \u1090 \u1077  \u1074 \u1077 \u1073 -\u1080 \u1085 \u1090 \u1077 \u1088 \u1092 \u1077 \u1081 \u1089  Airflow:\
{\listtext	4	}\uc0\u1055 \u1077 \u1088 \u1077 \u1081 \u1076 \u1080 \u1090 \u1077  \u1087 \u1086  \u1072 \u1076 \u1088 \u1077 \u1089 \u1091 \'a0{\field{\*\fldinst{HYPERLINK "http://localhost:8080/"}}{\fldrslt \cf4 \strokec4 http://localhost:8080}}\cb1 \
\pard\tx220\tx720\pardeftab720\li720\fi-720\partightenfactor0
\ls1\ilvl0\cf2 \cb3 {\listtext	5	}\uc0\u1051 \u1086 \u1075 \u1080 \u1085 :\'a0\cb5 airflow\cb1 \
\ls1\ilvl0\cb3 {\listtext	6	}\uc0\u1055 \u1072 \u1088 \u1086 \u1083 \u1100 :\'a0\cb5 airflow\cb1 \
\pard\tx220\tx720\pardeftab720\li720\fi-720\partightenfactor0
\ls1\ilvl0\cf2 \cb3 \kerning1\expnd0\expndtw0 \outl0\strokewidth0 \
{\listtext	8	}5. \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 \uc0\u1040 \u1082 \u1090 \u1080 \u1074 \u1080 \u1088 \u1091 \u1081 \u1090 \u1077  DAG:\cb1 \
\pard\tx220\tx720\pardeftab720\li720\fi-720\partightenfactor0
\ls1\ilvl0\cf2 \cb3 {\listtext	9	}\uc0\u1053 \u1072 \u1081 \u1076 \u1080 \u1090 \u1077  \u1074  \u1089 \u1087 \u1080 \u1089 \u1082 \u1077 \'a0\cb5 car_price_prediction\cb3 , \uc0\u1074 \u1082 \u1083 \u1102 \u1095 \u1080 \u1090 \u1077  \u1090 \u1091 \u1084 \u1073 \u1083 \u1077 \u1088  \u1080  \u1079 \u1072 \u1087 \u1091 \u1089 \u1090 \u1080 \u1090 \u1077  \u1074 \u1088 \u1091 \u1095 \u1085 \u1091 \u1102  \u1080 \u1083 \u1080  \u1076 \u1086 \u1078 \u1076 \u1080 \u1090 \u1077 \u1089 \u1100  \u1074 \u1099 \u1087 \u1086 \u1083 \u1085 \u1077 \u1085 \u1080 \u1103  \u1087 \u1086  \u1088 \u1072 \u1089 \u1087 \u1080 \u1089 \u1072 \u1085 \u1080 \u1102 .\cb1 \
}