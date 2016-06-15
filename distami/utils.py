# Copyright 2013 Answers for AWS LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import time
import boto
from boto import ec2

from distami.exceptions import * 

log = logging.getLogger(__name__)


def get_ami(conn, ami_id, ami_tags = None):
    ''' Gets a single AMI as a boto.ec2.image.Image object '''
    
    attempts = 0
    max_attempts = 5
    image_filters = {}
    if ami_tags is not None:
        for tag in ami_tags:
            kv = tag.split(":")
            if len(kv) == 2:
                image_filters["tag:" + kv[0]] = kv[1]
    else:
        image_filters = None

    while (attempts < max_attempts):
        try:
            attempts += 1
            images = conn.get_all_images(ami_id, filters=image_filters)
        except boto.exception.EC2ResponseError:
            msg = "Could not find AMI '%s' with tags %s in region '%s'" % (ami_id, ami_tags, conn.region.name)
            if attempts < max_attempts:
                # The API call to initiate an AMI copy is not blocking, so the
                # copied AMI may not be available right away
                log.debug(msg + ' so waiting 5 seconds and retrying')
                time.sleep(5)
            else:
                raise DistamiException(msg)
    
    log.debug("Found AMIs: %s", images)
    if len(images) != 1:
        msg = "Somehow more than 1 AMI was detected - this is a weird error"
        raise DistamiException(msg)
        
    return images[0]


def get_snapshot(conn, snapshot_id):
    ''' Gets a single snapshot as a boto.ec2.snapshot.Snapshot object '''
    
    try:
        snapshots = conn.get_all_snapshots(snapshot_id)
    except boto.exception.EC2ResponseError:
        msg = "Could not snapshot '%s' in region '%s'" % (snapshot_id, conn.region.name)
        raise DistamiException(msg)
    
    log.debug("Found snapshots: %s", snapshots)
    if len(snapshots) != 1:
        msg = "Somehow more than 1 snapshot was detected - this is a weird error"
        raise DistamiException(msg)
        
    return snapshots[0]


def get_regions_to_copy_to(source_region):
    ''' Gets the list of regions to copy an AMI to '''
    
    regions = []
    for region in ec2.regions():
        if region.name == source_region:
            continue
        # Filter out GovCloud
        if region.name == 'us-gov-west-1':
            continue
        # Filter out China
        if region.name == 'cn-north-1':
            continue
        regions.append(region.name)
        
    return regions


def wait_for_ami_to_be_available(conn, ami_id, ami_tags = None):
    ''' Blocking wait until the AMI is available '''
    
    ami = get_ami(conn, ami_id, ami_tags)
    log.debug('AMI details: %s', vars(ami))
    
    while ami.state != 'available':
        log.info("%s in %s not available, waiting...", ami_id, conn.region.name)
        time.sleep(30)
        ami = get_ami(conn, ami_id, ami_tags)
        
        if ami.state == 'failed':
            msg = "AMI '%s' is in a failed state and will never be available" % ami_id
            raise DistamiException(msg)
    
    return ami
