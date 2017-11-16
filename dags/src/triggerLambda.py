from __future__ import print_function
import requests
import json
import gzip
import boto3
import datetime
from googleads import AdWordsClient
import argparse
import re
import pygsheets
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

parser = argparse.ArgumentParser()

parser.add_argument('-c', '--client', type=str, required=True)
parser.add_argument('-a', '--account', type=str, required=True)
parser.add_argument('-s', '--start-date', default=datetime.datetime.now())
parser.add_argument('-e', '--engine', type=str, required=True)
parser.add_argument('-t', '--entity', type=str, required=True)
parser.add_argument('-m', '--email', type=str, required=True)

def read(client, account, entity, start_date, end_date):
    if entity == 'CAMPAIGN':
        query = """
                SELECT
                Date,
                AccountCurrencyCode,
                AccountDescriptiveName,
                AdNetworkType1,
                AdNetworkType2,
                AdvertisingChannelType,
                AllConversions,
                AllConversionValue,
                AveragePosition,
                BidType,
                BiddingStrategyId,
                BiddingStrategyName,
                BiddingStrategyType,
                CampaignId,
                CampaignName,
                CampaignStatus,
                Clicks,
                ContentBudgetLostImpressionShare,
                ContentImpressionShare,
                ContentRankLostImpressionShare,
                Conversions,
                ConversionValue,
                Cost,
                Ctr,
                Device,
                Impressions,
                PrimaryCompanyName,
                SearchExactMatchImpressionShare,
                SearchImpressionShare,
                SearchBudgetLostImpressionShare,
                SearchRankLostImpressionShare,
                TrackingUrlTemplate,
                UrlCustomParameters
            FROM
                CAMPAIGN_PERFORMANCE_REPORT
            WHERE
                CampaignStatus != "REMOVED"
            DURING
                {},{}
        """.format(start_date, end_date)
    elif entity == 'ADGROUP':
        query = """
            SELECT
            Date,
            AccountCurrencyCode,
            AccountDescriptiveName,
            AdGroupStatus,
            AdGroupId,
            AdGroupName,
            AllConversions,
            AllConversionValue,
            AveragePosition,
            BidType,
            BiddingStrategyId,
            BiddingStrategyName,
            BiddingStrategyType,
            CampaignId,
            CampaignName,
            CampaignStatus,
            Clicks,
            ContentImpressionShare,
            ContentRankLostImpressionShare,
            Conversions,
            ConversionValue,
            Cost,
            CpcBid,
            Ctr,
            Device,
            Impressions
        FROM
            ADGROUP_PERFORMANCE_REPORT
        WHERE
            CampaignStatus != "REMOVED"
            AND AdGroupStatus != "REMOVED"
        DURING
                {},{}
        """.format(start_date, end_date)
    elif entity == 'KEYWORD':
        query = """
            SELECT
            AdGroupId,
            AdGroupName,
            AdGroupStatus,
            CampaignId,
            CampaignName,
            CampaignStatus,
            Id,
            Criteria,
            Labels,
            Date,
            FinalUrls,
            Status
        FROM
            KEYWORDS_PERFORMANCE_REPORT
        WHERE
            CampaignStatus != "REMOVED"
            AND AdGroupStatus != "REMOVED"
            AND Status != "REMOVED"
        DURING
                {},{}
        """.format(start_date, end_date)
    elif entity == 'AD':
        query = """
            SELECT
            Date,
            AdGroupId,
            AdGroupName,
            AdType,
            Id,
            HeadlinePart1,
            HeadlinePart2,
            Description,
            Path1,
            Path2,
            CreativeFinalUrls,
            Status
        FROM
            AD_PERFORMANCE_REPORT
        WHERE
            AdGroupStatus != "REMOVED"
            AND Status != "DISABLED"
        DURING
                {},{}
        """.format(start_date, end_date)
    elif entity == 'NEGATIVE_KEYWORD':
        query = """
            SELECT
                CampaignName,
                SharedSetName,
                SharedSetType,
                Status
            FROM
                CAMPAIGN_SHARED_SET_REPORT
            WHERE
                SharedSetType = "NEGATIVE_KEYWORDS"
                and SharedSetName CONTAINS "Branded"
                and CampaignStatus != "REMOVED"
                and Status != "REMOVED"
            """
    else:
        return "Entity entered does not exist!!"
    adwords_client = AdWordsClient.LoadFromString("""adwords:\r
                                                  developer_token: mV9b2Dnaxf89F34GsIe-3A\r
                                                  user_agent: \"user_agent\"\r
                                                  client_id: 1083397655872-147m18r2och1a7hu0tqkuduqrp6n9qk3.apps.googleusercontent.com\r
                                                  client_secret: HqeXDjA3EzN9tfy5ax_P3f_W\r
                                                  refresh_token: 1/FPoJ5VEM4eE6qkIGGJn4vBGD-rT3sF3ILGTk1tM-eag""")
    adwords_client.SetClientCustomerId(account)
    downloader = adwords_client.GetReportDownloader(version='v201702')
    data = downloader.DownloadReportAsStringWithAwql(query,
                                                    'CSV',
                                                    skip_report_header=True,
                                                    skip_column_header=False,
                                                    skip_report_summary=True,
                                                    include_zero_impressions=True)

    return data

def write_google_sheet(data, entity):
    gc = pygsheets.authorize(outh_file='/home/airflow/docker-airflow/dags/src/sheets.googleapis.com-python.json')
    data_list = data.split("\n")
    l = []
    count = 0
    for i in data_list:
        if count < 1500:
            l.append(re.split(r',(?=(?:"[^"]*?(?: [^"]*)*))|,(?=[^",]+(?:,|$))', i))
            count = count + 1
        else:
            break
    labels = l[0]
    l = l[:-1]
    l = l[1:]
    df = pd.DataFrame.from_records(l, columns=labels)
    sht2 = gc.create("{} Report {}".format(entity, datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')), parent_id = "0B20mA0H6Dt4ZZ1p0SjlBeVBsdlU")
    wks = sht2.add_worksheet("Adwords")
    wks.set_dataframe(df,(1,1))
    header = wks.cell('A1')
    header.set_text_alignment(alignment='CENTER')
    header.text_format['bold'] = True
    header.update()

def send_email(subject, lines, slack_channel_email):
    """
    Send an e-mail to a slack channel with the default app-alerts Google e-mail address as the sender
    :param subject: Subject line of the e-mail
    :param lines: List of strings to make up the message body
    :param slack_channel_email: Target Slack channel to send the e-mail
    :return: None
    """
    # build a message for the given list and add new lines after each item

    body = 'This is a test email to let you know that report is done\n'

    email_subject = 'Subject: ' + subject

    message = '%s\n\n%s' % (email_subject, body)

    try:
        smtpObj = smtplib.SMTP("smtp.mailgun.org:587")
        smtpObj.starttls()
        smtpObj.login("dataalerts-dev@connex.rise.network", "xV-=8x65@81")
        smtpObj.sendmail("dataalerts-dev@connex.rise.network", slack_channel_email, message)
        print('Successfully sent e-mail to {}'.format(slack_channel_email))

    except Exception as e:
        print('Unable to send e-mail to Slack Channel ' + slack_channel_email + '. See error details below:')
        print(e)

if __name__ == "__main__":
    args = parser.parse_args()
    end_date = datetime.datetime.strptime(args.start_date, "%Y%m%d")
    start_date = (end_date - datetime.timedelta(days=1)).strftime('%Y%m%d')
    end_date = end_date.strftime('%Y%m%d')
    if args.engine == 'Adwords':
        data = read(args.client, args.account, args.entity, start_date, end_date)
        write_google_sheet(data, args.entity)
        send_email('test email', '', args.email)
