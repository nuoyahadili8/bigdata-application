#!/bin/bash
echo 'begin check IntegrationKafkaTopicWithStructStreaming server...'
application_name=`yarn application -list|grep b_yz_app_td |grep IntegrationKafkaTopicWithStructStreaming |grep "^application"|awk '{print $1}'`
if [ -z $application_name  ]; then
        echo 'IntegrationKafkaTopicWithStructStreaming server has been suspended.now begin start server.'
        sh /data/iop/projects/bin/bigdata-topic-integration.sh
fi
echo 'IntegrationKafkaTopicWithStructStreaming is Normal service'

echo 'begin check IntelligentSMSApplicationAuto server...'
application_name=`yarn application -list|grep b_yz_app_td |grep IntelligentSMSApplicationAuto |grep "^application"|awk '{print $1}'`
if [ -z $application_name  ]; then
        echo 'IntelligentSMSApplicationAuto server has been suspended.now begin start server.'
        sh /data/iop/projects/bin/IntelligentSMSApplicationAuto.sh
fi
echo 'IntelligentSMSApplicationAuto is Normal service'

echo 'begin check TourScenicAreaUsrInfoFromIntegrationTopic server...'
application_name=`yarn application -list|grep b_yz_app_td |grep TourScenicAreaUsrInfoFromIntegrationTopic |grep "^application"|awk '{print $1}'`
if [ -z $application_name  ]; then
        echo 'TourScenicAreaUsrInfoFromIntegrationTopic server has been suspended.now begin start server.'
        sh /data/iop/projects/bin/nm_tour.sh
fi
echo 'TourScenicAreaUsrInfoFromIntegrationTopic is Normal service'

echo 'begin check IntelligentSMSApplicationWithNo server...'
application_name=`yarn application -list|grep b_yz_app_td |grep IntelligentSMSApplicationWithNo |grep "^application"|awk '{print $1}'`
if [ -z $application_name  ]; then
        echo 'IntelligentSMSApplicationWithNo server has been suspended.now begin start server.'
        sh /data/iop/projects/bin/IntelligentSMSApplicationWithNo.sh
fi
echo 'IntelligentSMSApplication is Normal service'

echo 'begin check IntelligentSMSApplicationWithStayDuration server...'
application_name=`yarn application -list|grep b_yz_app_td |grep IntelligentSMSApplicationWithStayDuration |grep "^application"|awk '{print $1}'`
if [ -z $application_name  ]; then
        echo 'IntelligentSMSApplicationWithStayDuration server has been suspended.now begin start server.'
        sh /data/iop/projects/bin/IntelligentSMSApplicationWithStayDuration.sh
fi
echo 'IntelligentSMSApplicationWithStayDuration is Normal service'

echo 'begin check SendWelcomeToSelectPhone server...'
application_name=`yarn application -list|grep b_yz_app_td |grep SendWelcomeToSelectPhone |grep "^application"|awk '{print $1}'`
if [ -z $application_name  ]; then
        echo 'SendWelcomeToSelectPhone server has been suspended.now begin start server.'
        sh /data/iop/projects/bin/SendWelcomeToSelectPhone.sh
fi
echo 'SendWelcomeToSelectPhone is Normal service'