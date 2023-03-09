import boto3
import pymysql
import os
import time

asg_client = boto3.client('autoscaling', region_name=os.getenv('REGION'))
ec2_client = boto3.client('ec2', region_name=os.getenv('REGION'))

def get_volume_status():
    return ec2_client.describe_volumes(
        VolumeIds=[os.getenv('VOLUME_ID')]
    )['Volumes'][0]['State']


def lambda_handler(event, context):
    instance_id = event['detail']['instance-id']

    asg_instance_id = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[os.getenv('AUTOSCALING_GROUP_NAME')]
    )['AutoScalingGroups'][0]['Instances'][0]['InstanceId']

    if instance_id != asg_instance_id:
        return {
            'statusCode': 404,
            'body': None
        }

    asg_instance_ip = ec2_client.describe_instances(
        InstanceIds=[asg_instance_id]
    )['Reservations'][0]['Instances'][0]['PublicIpAddress']

    try:
        conn = pymysql.connect(
            host=asg_instance_ip,
            user=os.getenv("MYSQL_USER"),
            passwd=os.getenv("MYSQL_PASSWORD"),
            db=os.getenv("MYSQL_DATABASE"),
            connect_timeout=5
        )

        with conn.cursor() as cur:
            cur.execute("FLUSH TABLES WITH READ LOCK")
            cur.execute("SHUTDOWN")
        conn.commit()

    except pymysql.MySQLError as e:
        print(f"ERROR: Unexpected error: Could not connect to MySQL instance. Error: {e}")

    response = ec2_client.detach_volume(
        Device='/dev/sdh',
        InstanceId=asg_instance_id,
        VolumeId=os.getenv('VOLUME_ID')
    )

    state = get_volume_status()
    while state != 'available':
        state = get_volume_status()
        time.sleep(1)

    print(f"Detached volume from instance, response: {response}")

    response = asg_client.detach_instances(
        InstanceIds=[asg_instance_id],
        AutoScalingGroupName=os.getenv('AUTOSCALING_GROUP_NAME'),
        ShouldDecrementDesiredCapacity=False
    )

    response = ec2_client.terminate_instances(
        InstanceIds=[asg_instance_id]
    )

    print(f"Detached instance from ASG, response: {response}")

    return {
        'statusCode' : 200,
        'body': 'result'
    }