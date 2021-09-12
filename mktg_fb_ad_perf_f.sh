job_name='mktg_fb_ad_perf_f'
job_start_time=$(date +%s)
echo '+----------+----------+----------+----------+----------+'
echo 'Sourcing Files and assining Job Name variable'
source set_dwh_env_variables.sh
source ${dwh_common_base_dir}/set_dwh_common_variables.sh ${job_name}
source ${dwh_credentials_file_dir}/credentials.ctrl
source ${dwh_common_base_dir}/environment.ctrl
source ${dwh_common_base_dir}/nw_shell_modules/generic_job_functions.sh
source ${dwh_chef_credentials_file_dir}/passwords.ctrl
echo '+----------+----------+----------+----------+----------+'

trap : 0
echo >&2 '
********************************
*** '$job_name' LOAD STARTED ***
********************************
'
abort()
{
    echo >&2 '
**************************************
*** ERROR CODE '$job_name' ABORTED ***
**************************************
'
	bash ${dwh_common_base_dir}/dwh_job_fail_script.sh ${job_name}
    echo "An error occurred. Exiting while performing *****************"$Processing_Step >&2
    exit 1
}
trap 'abort' 0
set -e


echo_processing_step ${job_name} "Setting Variables, Running Preprocess scripts and Creating DIR Structures" "Started"
bash ${dwh_common_base_dir}/dwh_job_start_script.sh ${job_name}

echo '+----------+----------+----------+----------+----------+----------+'
echo '+----------+----------+---Custom Variables--+----------+----------+'
stage_db="dw_stage"
date_load_from=$1
date_load_to=$2
target_db='dw_report'
##### if user_date_range is empty then default is last 14 days
if [[ -n "${date_load_from}" && -n "${date_load_to}" ]]; then
    echo "date range defined by user"
    else
    date_load_from=`date --date='-9 day' '+%Y-%m-%d'`
    date_load_to=`date '+%Y-%m-%d'`
fi

config_file_name=${dwh_scripts_base_dir}/mktg_fb_ad_perf_f/pythonscripts/config.json
echo 'config_file_name   :-   '${config_file_name}
echo 'date_load_from:        :-   '${date_load_from}
echo 'date_load_to:        :-   '${date_load_to}
echo 'stage_db:        :-   '${stage_db}
echo 'target_db:        :-   '${target_db}
echo '+----------+----------+----------+----------+----------+----------+'

##################  functions  ##############################

fb_to_json()
{
echo "calling fb API and writing results as json file"
python ${dwh_scripts_base_dir}/mktg_fb_ad_perf_f/pythonscripts/fb_to_json.py -c ${config_file_name} -i ${Linux_Input} -o ${Linux_Input} -s ${date_load_from} -e ${date_load_to} --fb_app_id=${fb_app_id} --fb_app_secret=${fb_app_secret} --fb_access_token=${fb_access_token}
echo "json files are created under "${Linux_Input}
}

upload_to_S3()
{
local_file_nm=$1
S3_DateStamp=$2
python -c "from s3_modules import mv_to_s3 ; import s3_modules;  mv_to_s3('${local_file_nm}', '${S3_Events_Archive}/${S3_DateStamp}', '${Events_dwh_bucket}')"

# uploading file to s3 input to load to redshift
delete_if_exist_file=`echo ${local_file_nm} | cut -d '/' -f7 | cut -d '.' -f1`
delete_if_exist_file="mktg_fb_ad_perf_s.json"
sleep 2
echo "delete_if_exist_file "$delete_if_exist_file
python -c "from s3_modules import delete_key; delete_key('${S3_Events_Input}','${Events_dwh_bucket}','${delete_if_exist_file}')" || true
python -c "from s3_modules import mv_to_s3 ; import s3_modules;  mv_to_s3('${local_file_nm}', '${S3_Events_Input}', '${Events_dwh_bucket}')"
}

generate_manifest_file_in_S3()
{
export file_nm="'"$1"'"
echo "generate_manifest_file_in_S3 for "${file_nm}
python -c "from redshift_modules import generate_manifest_file ; generate_manifest_file('Prod','$Events_dwh_bucket','$job_name/input',${file_nm},'$job_name/manifest','2015-10-12','2016-05-01','modified')"
}

exec_truncate_copy_sql()
{
#export stage_db_nm="dw_stage"
export stage_table_nm=$1
export source_json_file_nm=$2
echo "stage_table_nm:"$stage_table_nm
echo "source_json_file_nm:"$source_json_file_nm

echo "Truncating stage table - "$stage_db"."$stage_table_nm
query_stage_delete="delete from "$stage_db"."$stage_table_nm" ;"
echo ${query_stage_delete}
jsonpaths_file_nm=`echo ${source_json_file_nm} | cut -d '.' -f1`

psql -h "$pdbHost" -p "$pport" -U "$pusername" -d "$pdatabase"  --single-transaction -c "$query_stage_delete"

echo "Copying data to stage table - "$stage_db"."$stage_table_nm" from "$source_json_file_nm
copy_query="copy "$stage_db"."$stage_table_nm"
from '"$s3_bucket_name"/"$job_name"/input/"${source_json_file_nm}"'
credentials '$s3_prod_load_creds'
JSON as '"$s3_bucket_name"/json/"$job_name"/"$jsonpaths_file_nm".jsonpaths'
maxerror as 1
ACCEPTINVCHARS COMPUPDATE ON
TRUNCATECOLUMNS
BLANKSASNULL
TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
;
"
psql -h "$pdbHost" -p "$pport" -U "$pusername" -d "$pdatabase"  -c "$copy_query"

}

bash ${dwh_common_base_dir}/setup_dir_structure.sh ${job_name}
echo_processing_step ${job_name} "Setting Variables, Running Preprocess scripts and Creating DIR Structures" "Completed"

echo '+----------+----------+----------+----------+----------+----------+'
echo '+----------+----------+----------+----------+----------+----------+'
echo '+----------+----- Starting to Process Main Script -----+----------+'
echo '+----------+----------+----------+----------+----------+----------+'
echo '+----------+----------+----------+----------+----------+----------+'

echo_processing_step ${job_name} "Removing Data Files" "Started"
list_job_related_files
find ${Linux_Input} -name \*fb*.json -exec rm {} \; || true
list_job_related_files
echo_processing_step ${job_name} "Removing Data Files" "Completed"

echo_processing_step ${job_name} "Download Fb Facebook" "Started"
fb_to_json
echo_processing_step ${job_name} "Download Fb Facebook" "Completed"

echo_processing_step ${job_name} "uploading all the files to S3 and stage table" "Started"
cd ${Linux_Input}
#ls ${Linux_Input}*fb*
#gzip -f *fb*.json
ls ${Linux_Input}*.json | sort | uniq > ${Linux_Output}/${job_name}_upload_files.list
#ls ${Linux_Input}*fb*.gz | sort | uniq > ${Linux_Output}/${job_name}_upload_files.list
cat ${Linux_Output}/${job_name}_upload_files.list

while read line
do
   echo "Uploading file to S3:"$line
   s3_upload_dt=`echo ${line} | cut -d '/' -f 7 | cut -d '_' -f 1`
   echo "s3_upload_dt: "$s3_upload_dt
   export S3_DateStamp=`date '+%Y%m%d'`
   echo "S3_DateStamp=$S3_DateStamp"

   S3_DateStamp=${S3_DateStamp:0:4}"/"${S3_DateStamp:4:2}"/"${S3_DateStamp:6:2}
   echo "S3_DateStamp from file =$S3_DateStamp"
   upload_to_S3 $line $S3_DateStamp
done  < ${Linux_Output}/${job_name}_upload_files.list

# file and stage table association
declare -A arr

arr["mktg_fb_ad_perf_s"]="mktg_fb_ad_perf_s.json"
for key in ${!arr[@]}; do
    echo ${key} ${arr[${key}]}
    exec_truncate_copy_sql ${key} ${arr[${key}]}
done

echo_processing_step ${job_name} "uploading all the files to S3 and stage table" "Complete"

echo '+----------+----------+----------+----------+----------+----------+'
echo '+----------+----------+----------+----------+----------+----------+'
echo '+----------+-----Completed Processiing Main Script-----+----------+'
echo '+----------+----------+----------+----------+----------+----------+'
echo '+----------+----------+----------+----------+----------+----------+'

#generate redshift param file
echo_processing_step ${job_name} "generate redshift param file" "notify"
echo "{
\"stage_db\":\"${stage_db}\",
\"target_db\":\"${target_db}\"
}" > ${Linux_Output}/${job_name}_redshift_params.json

cat ${Linux_Output}/${job_name}_redshift_params.json
# ETL scripts executed in sequence
echo "step1_refresh_mktg_fb_ad_perf_w.sql
step2_del_chg_mktg_fb_ad_perf_f.sql
step3_ins_new_mktg_fb_ad_perf_f.sql
step4_src_sys_id_resync.sql
step5_campaign_type_id_resync.sql
step6_vertical_id_resync.sql" | while read sql_script other_var
do
  echo_processing_step ${job_name} "$sql_script" "Started"
  bash ${dwh_common_base_dir}/redshift_sql_function.sh ${dwh_scripts_base_dir}/${job_name}/sqlfiles/${sql_script} ${Linux_Output}/${job_name}_redshift_params.json
  echo_processing_step ${job_name} "$sql_script" "Completed"
done

# DQ scripts executed in sequence
echo "mktg_fb_ad_spent_monthly_dq.json
mktg_fb_ad_perf_f_data_availability.json
mktg_fb_ad_campaign_lvl_data_compare.json" | while read script other_var
do
  Processing_Step="DQ check - ${script}"
  echo_processing_step ${job_name} "${Processing_Step}" "Started"
  python ${dwh_common_base_dir}/run_dq_check.py ${dwh_scripts_base_dir}/${job_name}/pythonscripts/${script}
  echo_processing_step ${job_name} "${Processing_Step}" "Completed"
done

echo_processing_step ${job_name} "Calling End Script" "Started"
bash ${dwh_common_base_dir}/dwh_job_end_script.sh ${job_name}
echo_processing_step ${job_name} "Calling End Script" "Completed"

job_end_time=$(date +%s)

echo "Job Completed in : "$(( ($job_end_time-$job_start_time) / ( 60) )) minutes, $(( ($job_end_time-$job_start_time) % 60 )) seconds
trap : 0
echo >&2 '
************************************
***  '$job_name' LOAD COMPLETED  ***
************************************
'
