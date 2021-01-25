package com.aerospike.client.reactor;

import com.aerospike.client.policy.*;

public interface DefaultPolicyProvider {

    Policy getReadPolicyDefault();

    WritePolicy getWritePolicyDefault();

    ScanPolicy getScanPolicyDefault();

    QueryPolicy getQueryPolicyDefault();

    BatchPolicy getBatchPolicyDefault();

    InfoPolicy getInfoPolicyDefault();
}
