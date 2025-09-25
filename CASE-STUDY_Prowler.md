# Prowler

TODO preamble about what it is
## Stats

### size

```bash
% ls -lsh prowler-output-813954463431-20250917114227.ocsf.json
121M -rw-rw-r-- 1 bg bg 121M Sep 18 08:12 prowler-output-813954463431-20250917114227.ocsf.json
```

### Counts

```bash
% cat prowler-output-813954463431-20250917114227.ocsf.json | jq 'length'
14790
```

## Initial prep

We only care about failing findings:
```bash
% cat prowler-output-813954463431-20250917114227.ocsf.json | jq '[.[] | select(.status_code == "FAIL")]' > failing.json
```

What categories do we have:
```bash
% cat failing.json | jq '.[].finding_info.title' -r | sort | uniq
Amazon EBS volumes should be protected by a backup plan.
Amazon EC2 launch templates should not assign public IPs to network interfaces.
... 140 elements cut ...
Secrets should be rotated periodically
Security Groups created by EC2 Launch Wizard.
There are High severity GuardDuty findings
```

Pick one that looks like we want to report on it: `Ensure no Network ACLs allow ingress from 0.0.0.0/0 to Microsoft RDP port 3389`


How many do we have:
```bash 
% cat failing.json | jq '[.[] | select(.finding_info.title == "Ensure no Network ACLs allow ingress from 0.0.0.0/0 to Microsoft RDP port 3389")]' | jq 'length'
10
```

Look at the structure of each one:
```bash

% cat failing.json | jq '[.[] | select(.finding_info.title == "Ensure no Network ACLs allow ingress from 0.0.0.0/0 to Microsoft RDP port 3389")]' | jq '.[0]' | gron
json = {};
json.activity_id = 1;
json.activity_name = "Create";
json.category_name = "Findings";
json.category_uid = 2;
json.class_name = "Detection Finding";
json.class_uid = 2004;
json.cloud = {};
json.cloud.account = {};
json.cloud.account.labels = [];
json.cloud.account.name = "";
json.cloud.account.type = "AWS Account";
json.cloud.account.type_id = 10;
json.cloud.account.uid = "813[redacted]431";
json.cloud.org = {};
json.cloud.org.name = "";
json.cloud.org.uid = "";
json.cloud.provider = "aws";
json.cloud.region = "ca-central-1";
json.finding_info = {};
json.finding_info.created_time = 1758120147;
json.finding_info.created_time_dt = "2025-09-17T11:42:27.501928";
json.finding_info.desc = "Ensure no Network ACLs allow ingress from 0.0.0.0/0 to Microsoft RDP port 3389";
json.finding_info.title = "Ensure no Network ACLs allow ingress from 0.0.0.0/0 to Microsoft RDP port 3389";
json.finding_info.types = [];
json.finding_info.types[0] = "Infrastructure Security";
json.finding_info.uid = "prowler-aws-ec2_networkacl_allow_ingress_tcp_port_3389-813[REDACTED]31-ca-central-1-acl-0e6ab7b039a6571ef";
json.message = "Network ACL acl-0e6ab7b039a6571ef has Microsoft RDP port 3389 open to the Internet.";
json.metadata = {};
json.remediation = {};
json.remediation.desc = "Apply Zero Trust approach. Implement a process to scan and remediate unrestricted or overly permissive network acls. Recommended best practices is to narrow the definition for the minimum ports required.";
json.remediation.references = [];
json.remediation.references[0] = "https://docs.aws.amazon.com/vpc/latest/userguide/vpc-network-acls.html";
json.resources = [];
json.resources[0] = {};
json.resources[0].cloud_partition = "aws";
json.resources[0].data = {};
json.resources[0].data.details = "";
json.resources[0].data.metadata = {};
json.resources[0].data.metadata.arn = "arn:aws:ec2:ca-central-1:813954463431:network-acl/acl-0e6ab7b039a6571ef";
json.resources[0].data.metadata.entries = [];
json.resources[0].data.metadata.entries[0] = {};
json.resources[0].data.metadata.entries[0].CidrBlock = "0.0.0.0/0";
json.resources[0].data.metadata.entries[0].Egress = true;
json.resources[0].data.metadata.entries[0].Protocol = "-1";
json.resources[0].data.metadata.entries[0].RuleAction = "allow";
json.resources[0].data.metadata.entries[0].RuleNumber = 100;
json.resources[0].data.metadata.entries[1] = {};
...
json.resources[0].group = {};
json.resources[0].group.name = "ec2";
json.resources[0].labels = [];
json.resources[0].name = "acl-0e6ab7b039a6571ef";
json.resources[0].region = "ca-central-1";
json.resources[0].type = "AwsEc2NetworkAcl";
json.resources[0].uid = "arn:aws:ec2:ca-central-1:813954463431:network-acl/acl-0e6ab7b039a6571ef";
json.risk_details = "Even having a perimeter firewall, having network acls open allows any user or malware with vpc access to scan for well known and sensitive ports and gain access to instance.";
json.severity = "Medium";
json.severity_id = 3;
json.status = "New";
json.status_code = "FAIL";
json.status_detail = "Network ACL acl-0e6ab7b039a6571ef has Microsoft RDP port 3389 open to the Internet.";
json.status_id = 1;
json.time = 1758120147;
json.time_dt = "2025-09-17T11:42:27.501928";
json.type_name = "Detection Finding: Create";
json.type_uid = 200401;
json.unmapped = {};
...
```


Then we figure out the jq query for each element we care about, and pull them in.

See test/examples/prowler* 
