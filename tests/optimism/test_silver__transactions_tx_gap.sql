-- depends_on: {{ ref('silver__blocks') }}
{{ fsc_utils.tx_gaps(ref("silver__transactions")) }}
