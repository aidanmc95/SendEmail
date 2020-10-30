#!/usr/bin/env python

"""
Use this script to email individual results to patients through the
clinical@mobilexpressclinics.com gmail account.

This fits into the utils pipeline. It uses the output of requisition_roster
to find emails, requisition/accession IDs, and 

Reference:
This is based on a (incomplete and misleading) Python Quickstart:
https://developers.google.com/gmail/api/quickstart/python
"""

from __future__ import print_function

from apiclient import errors
import base64
import csv
from email.mime.application import MIMEApplication
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mimetypes
import pickle
import os
import sys

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


patient_list_fp = sys.argv[1]   # Use the reqs.txt file, need 'req_id' and 'email'
output_fp = sys.argv[2]
report_dir = sys.argv[3]        # Contains PDFs
collection_date = sys.argv[4]   # YYYY-MM-DD
testing_lab = 'Certis'        # diacarta or certis, case insensitive
if len(sys.argv) > 5:
    testing_lab = sys.argv[5].lower()

ignore_fp = None                # needs column named 'req_id'
if len(sys.argv) > 6:
    ignore_fp = sys.argv[6]

reqs_to_ignore = []
if ignore_fp is not None:
    if not os.path.exists(ignore_fp):
        sys.stderr.write("No requisition ignore list {}\n".format(reqs_fp))
        exit(1)
    sys.stdout.write("Ignoring reqs in {}\n".format(ignore_fp))
    with open(ignore_fp, 'r') as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            reqs_to_ignore.append(row['req_id'])
        f.close()
    sys.stdout.write("{} reqs will be skipped\n".format(len(reqs_to_ignore)))

patients = []
with open(patient_list_fp, 'r') as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        if row['req_id'] in reqs_to_ignore:
            sys.stdout.write("skipping {}\n".format(row['req_id']))
        else:
            sys.stdout.write("planning {}\n".format(row['req_id']))
            patients.append(row.copy())
    f.close()

"""Gmail Utilities"""
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.compose']


def encode_message(message):
    b64_bytes = base64.urlsafe_b64encode(message.as_bytes())
    b64_string = b64_bytes.decode()
    body = {'raw': b64_string}
    return body


def create_draft(service, user_id, message_body):
    """Create and insert a draft email. Print the returned draft's message and id.

    Args:
        service: Authorized Gmail API service instance.
        user_id: User's email address. The special value "me"
        can be used to indicate the authenticated user.
        message_body: The body of the email message, including headers.

    Returns:
        Draft object, including draft id and message meta data.
    """
    try:
        message = {'message': message_body}
        draft = service.users().drafts().create(userId=user_id, body=message).execute()

        sys.stdout.write(
            "Draft id: {}\nDraft message: {}\n".format(
                draft['id'], draft['message']
            )
        )

        return draft
    except:
        sys.stdout.write("An error occurred: {}\n".format(error))
        return None


def send_message(service, user_id, message):
    """Send an email message.

    Args:
        service: Authorized Gmail API service instance.
        user_id: User's email address. The special value "me"
        can be used to indicate the authenticated user.
        message: Message to be sent.

    Returns:
        Sent Message.
    """
    #sys.stdout.write("Message: {}\n".format(message))
    message = (service.users().messages().send(userId=user_id, body=message)
            .execute())
    sys.stdout.write("Message Id: {}\n".format(message['id']))
    return message


def create_message(sender, to, subject, message_text, file = None):
    """Create a message for an email.

    Args:
        sender: Email address of the sender.
        to: Email address of the receiver.
        subject: The subject of the email message.
        message_text: The text of the email message.
        file: The path to the file to be attached.

    Returns:
        An object containing a base64url encoded email object.
    """
    message = MIMEMultipart()
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject

    msg = MIMEText(message_text)
    message.attach(msg)

    if file:
        content_type, encoding = mimetypes.guess_type(file)
        sys.stdout.write("guessed type: {}, encoding: {}\n".format(
            content_type, encoding
        ))

        if content_type is None or encoding is not None:
            content_type = 'application/octet-stream'

        main_type, sub_type = content_type.split('/', 1)
        if main_type == 'text':
            fp = open(file, 'rb')
            msg = MIMEText(fp.read(), _subtype=sub_type)
            fp.close()
        elif main_type == 'image':
            fp = open(file, 'rb')
            msg = MIMEImage(fp.read(), _subtype=sub_type)
            fp.close()
        elif main_type == 'audio':
            fp = open(file, 'rb')
            msg = MIMEAudio(fp.read(), _subtype=sub_type)
            fp.close()
        elif content_type == "application/pdf":
            fp = open(file, 'rb')
            msg = MIMEApplication(fp.read(), _subtype=sub_type)
            fp.close()
        else:
            fp = open(file, 'rb')
            msg = MIMEBase(main_type, sub_type)
            msg.set_payload(fp.read())
            fp.close()
        filename = os.path.basename(file)
        msg.add_header('Content-Disposition', 'attachment', filename=filename)
        message.attach(msg)

    return encode_message(message)


"""Bread and butter"""
def send_patient_result(service, patient_email, subject, body, pdf_path):
    if patient_email.strip() == "":
        return False
    
    message = create_message(
        "clinical@mxcglobal.com", 
        patient_email, 
        subject, 
        body,
        pdf_path
    )
    send_message(service, "clinical@mxcglobal.com", message)


collected_patients = []
missed_patients = []
def main():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)

    email_body = """Hello,

Please find your test results from {} attached.

Warm Regards,
Mobile Xpress Clinics
Clinical Services
www.mobilexpressclinics.com

Confidentiality Notice: This message, together with any attachments, is intended only for the use of the individual or entity to which it is addressed and may contain confidential or privileged information. If you think you have received this message in error, please advise the sender and then delete this message and any attachments immediately.
""".format(collection_date)

    output_keys = []
    for patient in patients:
        input_path = get_report_path(patient['req_id'])
        try:
            send_patient_result(
                service,
                patient['email'], 
                "Your COVID-19 Test Results", 
                email_body,
                input_path
            )
            patient['file_found'] = True
            if len(output_keys) == 0:
                output_keys = [ k for k in patient.keys() ]
            collected_patients.append(patient.copy())
        except FileNotFoundError:
            sys.stderr.write("Missing file {}\n".format(input_path))
            patient['file_found'] = False
            if len(output_keys) == 0:
                output_keys = [ k for k in patient.keys() ]
            missed_patients.append(patient.copy())

    with open(output_fp, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=output_keys, delimiter="\t")
        writer.writeheader()
        for patient in collected_patients:
            writer.writerow(patient)
        for patient in missed_patients:
            writer.writerow(patient)
        f.close()


if __name__ == '__main__':
    main()