import requests
import pandas as pd
import json
import pyodbc
import snowflake.connector
from fuzzywuzzy import fuzz
import re

def get_zoominfo_authentication():
    url='https://api.zoominfo.com/authenticate'
    json = {
            "username": "ENTER USERNAME", \
             "password": "ENTER PASSWORD" \
            };
    r=requests.post(url=url,json=json)
    return r

# Company Search

def get_zoominfo_company_id(login_key,row_num,company_name,industry,retry=1,skip_industry=0):
    url = 'https://api.zoominfo.com/search/company'
    if skip_industry == 1:
        json = {
            "companyName": company_name,
            "country": "US",
            "excludeDefunctCompanies": True
        };
    else:
        json = {
                "companyName":company_name,
                "country":"US",
            "excludeDefunctCompanies": True,
            "industryKeywords":str(industry).split(',')[1]
        };
    headers={"Content-Type":"application/json","Authorization":"Bearer "+login_key}
    r = requests.post(url=url, json=json,headers=headers)
    if r.status_code==200:
        print("\n company id retireved for "+company_name + " row num : "+ str(row_num))
        print(r.text)
        if len(r.json()['data'])> 0:
            return r.json()['data'][0]['id']
        else:
            get_zoominfo_company_id(login_key, row_num, company_name, industry, retry=0,skip_industry=1)

    elif retry==1:
        print("\n login token expired - retrying!! in get company id")
        loginstatus=get_zoominfo_authentication();
        login_key=json.loads(loginstatus.text)['jwt'];
        return get_zoominfo_company_id(login_key,row_num,company_name,industry,retry=0)


# Contact Search

def get_contact_data_search_api(page,radius,zipcode,industry,company_name,titles,email,phone,name,contacts_per_site,req_contacts_ind,retry=1):
    global login_key
    url = 'https://api.zoominfo.com/search/contact'

    body = {
        "locationSearchType": "PersonOrHQ",
        "department": "Operations",
         "country": "US",

        "managementLevel": "manager,director,VP Level Exec",
        "contactAccuracyScoreMin" : '85',
        "rpp":10, # rpp = results per page , 50 * 30 is 1500
        "revenueMin": 1000000
     };

    if page is not None:
        body["page"]=page
    if radius is not None:
        body["zipCodeRadiusMiles"] = str(radius)
    if zipcode is not None:
        body['zipCode']=str(zipcode)
    if industry is not None:
        body["industryCodes"]=industry
    if company_name is not None:
        body["companyName"]=company_name
    if titles is not None:
        body['jobTitle']=titles
    if email is not None:
        body['emailAddress']=email
    if phone is not None:
        body["phone"]=phone
    if name is not None:
        body["fullName"]=name
    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + login_key}
    r = requests.post(url=url, json=body, headers=headers)
    if r.status_code == 200:

       return pd.DataFrame.from_dict(r.json()['data'])

    elif r.text == '{"success":false,"error":"Page number (page) requested is greater than the available results.","statusCode":400}':
        return []
    elif retry == 1:
        print("\n login token expired - retrying!! in get company id")
        loginstatus = get_zoominfo_authentication();

        login_key = json.loads(loginstatus.text)['jwt'];

        return get_contact_data_search_api(page,radius,zipcode,industry,company_name,titles,email,phone,name,contacts_per_site,req_contacts_ind,0)
    pass

def get_contact_data_search_companyid_api(page,radius,zipcode,industry,companyid,contacts_per_site,req_contacts_ind,rpp,retry=1):
    global login_key
    url = 'https://api.zoominfo.com/search/contact'

    body = {
        "locationSearchType": "PersonOrHQ",
        "department": "Operations",
         "country": "US",

        "managementLevel": "manager,director,VP Level Exec",
        "contactAccuracyScoreMin" : '85',
        "rpp":rpp,
        #"revenueMin": 1000000,
        "companyid":companyid
     };

    if page is not None:
        body["page"]=page
    if radius is not None:
        body["zipCodeRadiusMiles"] = str(radius)
    if zipcode is not None:
        body['zipCode']=str(zipcode)
    if industry is not None:
        body["industryCodes"]=industry

    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + login_key}
    r = requests.post(url=url, json=body, headers=headers)
    if r.status_code == 200:

       return pd.DataFrame.from_dict(r.json()['data'])

    elif r.text == '{"success":false,"error":"Page number (page) requested is greater than the available results.","statusCode":400}':
        return []
    elif retry == 1:
        print("\n login token expired - retrying!! in get company id")
        loginstatus = get_zoominfo_authentication();

        login_key = json.loads(loginstatus.text)['jwt'];

        return get_contact_data_search_companyid_api(page,radius,zipcode,industry,companyid,contacts_per_site,req_contacts_ind,rpp,0)
    pass


def get_intent_data_search_api(page,radius,zipcode,industry,company_name,retry=1):
    global login_key
    url = 'https://api.zoominfo.com/search/intent'

    body = {
        "topics": [
                 #"Commercial Cleaning",
                    "Commercial Pest Control",
                 # "Insect Extermination {Rollins}",
                 #   "Office Pest Control {Rollins}"
                    "Pest Control General {Rollins}",
                #"Rodent/Small Animal Extermination {Rollins}"
                    ],
        "locationSearchType": "PersonOrHQ",

        "country": "US",
        "audienceStrengthMin": "B",
        "audienceStrengthMax": "A",
        "signalScoreMin": 80,
        "rpp": 10,
        "findRecommendedContacts": True,
        "sortBy": "signalScore",
        #"revenueMin": 1000000,
        "sortOrder": "desc"
    };

    body["signalStartDate"]="2022-09-01"
    body["signalEndDate"]="2022-10-31"
    if radius is not None:
        body["zipCodeRadiusMiles"] = str(radius)
    if zipcode is not None:
        body['zipCode']=str(zipcode)
    if industry is not None:
        body["industryCodes"]=industry
    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + login_key}

    r = requests.post(url=url, json=body, headers=headers)
    if r.status_code == 200:

        return pd.DataFrame.from_dict(r.json()['data'])

    elif r.text == '{"success":false,"error":"Page number (page) requested is greater than the available results.","statusCode":400}':
        return []
    elif retry == 1:
        print("\n login token expired - retrying!! in get company id")
        loginstatus = get_zoominfo_authentication();

        login_key = json.loads(loginstatus.text)['jwt'];

        return get_intent_data_search_api(page,radius,zipcode,industry,company_name,0)
    pass







    pass

def get_customers_in_zip_codes(zips):
    query='''select distinct
            CT.CT_ID as Contract_ID, 
            
            CT.CT_SLD_DATE as Contract_Sold_Date,
            COALESCE(ct.CT_TERM_DATE, ct.CT_EXP_DATE, ct.CT_CNCL_DATE) as contract_cancel_date, 
            CUST.CUST_NAME as Customer_Name,
            CUST.CUST_PA_NAME as Customer_PA_Name,
            ST.SVC_TYPE_CA_DESC ,
            SF.SVC_FREQ_DTL_DESC ,
            FCT.LEAD_AMT as Start_AMT,
            upper(trim(CUST.SVC_CUST_ADR_LINE_1)) as prospect_address_1,
                    lpad(upper(trim(CUST.SVC_CUST_ZIP)),5,0) as prospect_zip_1,
            upper(trim(CUST.BILL_ADR_LINE_1)) as prospect_bill_address_1,
                    --upper(COALESCE(trim(BILL_ADR_LINE_2),''))||'|'||
                    lpad(upper(trim(CUST.BILL_ZIP)),5,0) as prospect_bill_zip_1
             from 
            
             MKTG.ANALYTICS.VW_DIM_CUST CUST   
            -- region STREET_ADDRESS_1 Line 1 Match to Bill/ Serv STREET_ADDRESS_1
                
            -- endregion
             
            INNER JOIN MKTG.ANALYTICS.VW_FCT_CT FCT ON  FCT.CUST_SKEY = CUST.CUST_SKEY  		
             
              INNER JOIN MKTG.ANALYTICS.VW_DIM_CT CT  ON CT.CT_SKEY = FCT.CT_SKEY 
                and (ct.CT_SLD_DATE between '2010-01-01' and '2022-09-26') 
                
            
              
              INNER JOIN MKTG.ANALYTICS.VW_DIM_ORG_FN O ON O.ORG_FN_SKEY = FCT.ORG_FN_SVC_SKEY
                and o.ORG_FN_LVL0_NAME='Orkin'
              
              inner join MKTG.ANALYTICS.VW_DIM_SVC_TYPE st on st.SVC_TYPE_SKEY = fct.SVC_TYPE_SKEY 
                and st.ACCT_TYPE_DESC ='Commercial'
              
              inner join MKTG.ANALYTICS.VW_DIM_SVC_FREQ sf on sf.SVC_FREQ_SKEY = fct.SVC_FREQ_SKEY
            where 
            
            CT_STAT_CODE in ('P','A')
            and 
            (lpad(upper(trim(CUST.SVC_CUST_ZIP)),5,0) in (''' + zips+ ''')
                
                
                or 	
                 lpad(upper(trim(CUST.BILL_ZIP)),5,0) in ('''+ zips+ ''')
                  
                )''';
    
    # ENTER SNOWFLAKE INFORMATION BELOW

    conn = snowflake.connector.connect(
        user="ENTER SNOWFLAKE USERNAME",
        password="ENTER SNOWFLAKE PASSWORD",
        account="ENTER SNOWFLAKE ACCOUNT INFO",
        warehouse="ENTER WAREHOUSE",
        database="ENTER DB",
        schema="ENTER SCHEMA",
        role='ENTER ROLE'
    )

    cur = conn.cursor()

    # Execute a statement that will generate a result set.

    cur.execute(query)
    # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
    data_pd = cur.fetch_pandas_all()

    return data_pd

def get_prospects_in_zip_code(zips):
    query = '''select  distinct 
                emp.employeenumber,
                empname = case when NULLIF(empcnt.businessname, '') IS NULL THEN empcnt.firstname + ' ' + empcnt.lastname ELSE empcnt.businessname END,
                pr.prospectid,
                prospectname = case when NULLIF(cust.businessname, '') IS NULL THEN cust.firstname + ' ' + cust.lastname ELSE cust.businessname END,
                prospectemail = cust.email,
                pr.insertdate,
                
                pr.campaignid,
                psiteid = pr.siteid,
                csiteid = cust.siteid,
                psiteaddrid=pr.siteaddrid,
                saddressid=s.addressid,
                isnull(addr.streetnumber, cust.streetnumber)+' '+isnull(addr.predirection, cust.predirection)+' '+isnull(addr.streetname, cust.streetname) as streetname,
                isnull(addr.postdirection, cust.postdirection) as postdirection,
                isnull(addr.secondaryaddress, cust.secondaryaddress) as secondaryaddress,
                isnull(addr.city, cust.city) as city,
                isnull(addr.state, cust.state) as [state],
                isnull(addr.postalcode, cust.zip) as postalcode,
                branchnum=br.glprefix,
                branchname=br.branchname,
                divisionname=dis.categoryname,
                regionname=reg.categoryname
            from tssprospects pr
            left join tbranch br on pr.servicecenterid=br.branchid
            outer apply (select top 1 c.firstname, c.lastname, c.businessname, c.email, c.siteid, c.propertytype, c.streetname, c.streetnumber,
            c.predirection, c.postdirection, c.zip, c.city, c.state, c.secondaryaddress  from tsscustomerprospect cp join tsscustomers c on c.customerid=cp.customerid where cp.prospectid=pr.prospectid order by cp.customerprospectid desc) cust
            left join temployee emp on emp.employeeid=pr.salespersonid
            left join tcontact empcnt on empcnt.contactid=emp.contactid
            left join tbranchcategory dis on br.branchdistrictid = dis.branchcategoryid and dis.companyid=br.companyid
            left join tbranchcategory reg on br.branchregionid = reg.branchcategoryid and reg.companyid=br.companyid
            left join tsite s on s.siteid = (case when pr.siteid>0 then pr.siteid when cust.siteid>0 then cust.siteid else 0 end)
            left join taddress addr on addr.addressid = (case when pr.siteid>0 then s.addressid when cust.siteid>0 then s.addressid when pr.siteaddrid>0 then pr.siteaddrid else 0 end)
            
            where 1=1
            and  pr.deletedate is null
            and pr.rejecteddate is null
            and case when NULLIF(cust.businessname, '') IS NULL THEN cust.firstname + ' ' + cust.lastname ELSE cust.businessname END not like '@@%'
            and isnull(addr.postalcode, cust.zip) in (''' + zips + ''')'''

    cnxn = pyodbc.connect(
        r'Driver={SQL Server};Server=ENTER SERVER INFO;Database=ENTER DB INFO;Trusted_Connection=yes;')
    df = pd.read_sql_query(query, cnxn)
    return df

def run_title_match(contacts,titles):
    contacts['titlematchscore'] = contacts.apply(lambda x: max([fuzz_matcher(x['jobTitle'], i) for i in titles.split(',')]), axis=1)
    contacts=contacts[contacts['titlematchscore']>=75]
    return contacts.copy(deep=True)
    pass

def run_address_match(addr1,addr2):
    if not(pd.isna(addr2) or pd.isna(addr1)):
        try:
            if addr1.split()[0] == addr2.split()[0]: #street numbers match
                if fuzz_matcher(addr1,addr2) >= 75:
                    return True
        except:
            return False
    return False

    pass

def run_prospect_matchback(enriched_contacts):
    if bypass_matchback==False:
        zips = ','.join((map(lambda x: "'"+str(x).zfill(5)+"'",list(enriched_contacts.zipCode.unique()))))
        prospects_in_zips=get_prospects_in_zip_code(zips)

        if ( len(prospects_in_zips)>0):
            prospects_in_zips['postalcode'] = prospects_in_zips.postalcode.apply(lambda x: str(x).zfill(5))
            enriched_contacts['zipCode']=enriched_contacts.zipCode.apply(lambda x: str(x).zfill(5))
            merge = enriched_contacts.merge(prospects_in_zips,left_on = ['zipCode'],right_on=['postalcode'])
            if len(merge) > 0:
                merge['match'] = merge.apply(lambda x: run_address_match(x['street'],x['streetname']),axis=1)
                merge=merge[merge['match']==True]
                return enriched_contacts[~enriched_contacts.id.isin(merge.id.unique())]
    return enriched_contacts.copy(deep=True)
    pass


def run_customer_matchback(enriched_contacts):
    if bypass_matchback == False:
        zips = ','.join((map(lambda x: "'"+str(x).zfill(5)+"'",list(enriched_contacts.zipCode.unique()))))
        customers_in_zips=get_customers_in_zip_codes(zips)


        if ( len(customers_in_zips)>0):
            customers_in_zips['PROSPECT_ZIP_1'] = customers_in_zips.PROSPECT_ZIP_1.apply(lambda x: str(x).zfill(5))
            customers_in_zips['PROSPECT_BILL_ZIP_1']=customers_in_zips.PROSPECT_BILL_ZIP_1.apply(lambda x: str(x).zfill(5))
            enriched_contacts['zipCode']=enriched_contacts.zipCode.apply(lambda x: str(x).zfill(5))
            merge = enriched_contacts.merge(customers_in_zips,left_on = ['zipCode'],right_on=['PROSPECT_ZIP_1'])
            if len(merge)>0:
                merge['match'] = merge.apply(lambda x: run_address_match(x['street'],x['PROSPECT_ADDRESS_1']),axis=1)
                merge=merge[merge['match']==True]
                filter1=enriched_contacts[~enriched_contacts.id.isin(merge.id.unique())]
                merge = filter1.merge(customers_in_zips, left_on=['zipCode'], right_on=['PROSPECT_ZIP_1'])
 # comment out this section if you want to remove bnbill addrees
  #              if len(merge) > 0:
  #                  merge['match'] = merge.apply(lambda x: run_address_match(x['street'], x['PROSPECT_BILL_ADDRESS_1']), axis=1)
  #                  merge = merge[merge['match'] == True]
  #                  filter2 = filter1[~filter1.id.isin(merge.id.unique())]

#                    return filter2.copy(deep=True)
    # commenr tiill here uncommeht this : 
                return filter1.copy(deep=True)

    return enriched_contacts.copy(deep=True)

    pass



def get_branches_in_region(regionnum):
    return branch_metadata[branch_metadata.REGNUM == regionnum]['BRCODE']
    pass

def get_branch_zipcode(branchnum):
    return branch_metadata[branch_metadata.BRCODE == branchnum].BRZIPCODE.values[0]
    pass

def get_region_metadata(regionnum):
    return region_metadata_file[region_metadata_file['Regnum'] == regionnum]
    pass

def get_branch_am_count(branchnum):
    return branch_metadata[branch_metadata.BRCODE == branchnum].budgetam
    pass
def get_all_am_count():
    return sum(branch_metadata.budgetam)
    pass

def get_contact_data_enrich_api(ids):
    out=[]
    for id in ids:
        out.append(get_contact_data_enrich_api_single_contact(id))
    if len(out)>0:
        out=pd.concat(out)
    return out
def get_contact_data_enrich_api_single_contact(id,retry=1):
    global login_key
    url = 'https://api.zoominfo.com/enrich/contact'

    body = {
            "outputFields": [
        "id",
        "firstName",
        "middleName",
        "lastName",
        "email",
		"hasCanadianEmail",
		"phone",
		"directPhoneDoNotCall",
        "street",
        "city",
        "region",
        "metroArea",
        "zipCode",
        "state",
        "country",
        "personHasMoved",
        "withinEu",
        "withinCalifornia",
        "withinCanada",
        "lastUpdatedDate",
        "noticeProvidedDate",
        "salutation",
        "suffix",
        "jobTitle",
        "jobFunction",
        "companyDivision",
        "education",
        "hashedEmails",
        "picture",
		"mobilePhoneDoNotCall",
        "externalUrls",
        "companyId",
        "companyName",
        "companyDescriptionList",
        "companyPhone",
        "companyFax",
        "companyStreet",
        "companyCity",
        "companyState",
        "companyZipCode",
        "companyCountry",
        "companyLogo",
        "companySicCodes",
        "companyNaicsCodes",
        "contactAccuracyScore",
        "companyWebsite",
        "companyRevenue",
        "companyRevenueNumeric",
        "companyEmployeeCount",
        "companyType",
        "companyTicker",
        "companyRanking",
        "isDefunct",
        "companySocialMediaUrls",
        "companyPrimaryIndustry",
        "companyIndustries",
        "companyRevenueRange",
        "companyEmployeeRange",
        "employmentHistory",
        "managementLevel",
        "locationCompanyId"
    ],
            "matchPersonInput": [
                {
                    "personId": str(id)
                }
            ]
    };


    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + login_key}
    r = requests.post(url=url, json=body, headers=headers)
    if r.status_code == 200:

        return pd.DataFrame.from_dict(r.json()['data']['result'][0]['data'])


    elif retry == 1:
        print("\n login token expired - retrying!! in get company id")
        loginstatus = get_zoominfo_authentication();

        login_key = json.loads(loginstatus.text)['jwt'];
        return get_contact_data_enrich_api_single_contact(id, retry=0)
    pass

def fuzz_matcher(add1,add2,score_thresh=75):
    score=fuzz.token_set_ratio(add1,add2)
    return score



def get_industry_mha_contacts(industry,req_contacts_ind,titles,zipcode,contacts_per_site=1):
    num_contacts = 0
    num_loops = 2 # max nubler of pages zoominfo api is 30
    loops=0
    page=0
    company_contacts_not_chosen=[]
    exclude_company_ids=[]
    contacts_filtered = []
    contacts_filtered_propects=[]
    #loginstatus = get_zoominfo_authentication();
    #login_key = json.loads(loginstatus.text)['jwt']
    while num_contacts<= req_contacts_ind and loops <= num_loops:
        #get_contact_data_search_api(login_key,page,radius,zipcode,industry,company_name,titles,email,phone,name,contacts_per_site,req_contacts_ind):
        #get intent companies

        contact_search  = get_contact_data_search_api(page,50,zipcode,industry, None, None, None, None, None,contacts_per_site,req_contacts_ind)

        if len(contact_search)>0:
            # filter out companies we dont need
            contact_search = contact_search[contact_search.company.apply(lambda x: x['id'] not in exclude_company_ids)]
            if len(contact_search)>0:
                contacts_filtered_titles=run_title_match(contact_search,titles)
                contacts_filtered_titles = contacts_filtered_titles[contacts_filtered_titles['hasEmail'] | contacts_filtered_titles['hasMobilePhone']] # or
                # append older lower rank contacts if their company is not excluded

                # get company id for contact

                if len(contacts_filtered_titles)>0:
                    contacts_filtered_titles['companyid'] = contacts_filtered_titles.company.apply(lambda x: x['id'])
                    contacts_filtered_titles = pd.concat([contacts_filtered_titles, company_contacts_not_chosen]) if len(
                        company_contacts_not_chosen) > 0 else contacts_filtered_titles
                    # get contacts ranked by
                    contacts_filtered_titles['rank'] = contacts_filtered_titles.groupby('companyid')['titlematchscore'].cumcount(ascending=False)+1
                    temp=contacts_filtered_titles[contacts_filtered_titles['rank'] == 1].head(req_contacts_ind-num_contacts)
                    company_contacts_not_chosen=pd.concat([company_contacts_not_chosen,contacts_filtered_titles[~contacts_filtered_titles.id.isin(temp.id.unique())]]) if len(company_contacts_not_chosen)>0 else contacts_filtered_titles[~contacts_filtered_titles.id.isin(temp.id.unique())];
                    contacts_filtered_titles = temp#contacts_filtered_titles[contacts_filtered_titles['rank'] == 1]
                    #contacts_filtered_titles=contacts_filtered_titles
                    enriched_contacts = get_contact_data_enrich_api(contacts_filtered_titles.id.values)
                    if len(enriched_contacts)>0:

                        contacts_filtered_customers=run_customer_matchback(enriched_contacts)
                        if len(contacts_filtered_customers) > 0:
                            contacts_filtered_propects=run_prospect_matchback(contacts_filtered_customers)
                            if len(contacts_filtered_propects) > 0:
                                contacts_filtered = contacts_filtered_propects \
                                if len(contacts_filtered) == 0 else \
                                pd.concat([contacts_filtered_propects, contacts_filtered]) \
                                if len(contacts_filtered_propects) > 0 else contacts_filtered_propects
                                # add companies for whom we  found contacts to the exclusion list
                                exclude_company_ids=contacts_filtered.companyid.unique()
                                num_contacts+=len(contacts_filtered)
                                #remove lower rank contacts whose companies are excluded
                                company_contacts_not_chosen=company_contacts_not_chosen[company_contacts_not_chosen.companyid.apply(lambda x: x not in exclude_company_ids)]
            else:
                break
        else:
            break
        page=page+1
        loops+=1
    return contacts_filtered




def get_industry_amlist_contacts(radius,zipcode,industry,company_name,titles,email,phone,name,req_contacts_ind,contacts_per_site=1):
    num_contacts = 0
    num_loops = 2
    loops=0
    page=0
    company_contacts_not_chosen=[]
    exclude_company_ids=[]
    contacts_filtered = []
    contacts_filtered_propects=[]
    #loginstatus = get_zoominfo_authentication();
    #login_key = json.loads(loginstatus.text)['jwt']
    while num_contacts<= req_contacts_ind and loops <= num_loops:
        #get_contact_data_search_api(login_key,page,radius,zipcode,industry,company_name,titles,email,phone,name,contacts_per_site,req_contacts_ind):
        #get intent companies

        contact_search  = get_contact_data_search_api( page,radius,zipcode,industry,company_name,titles,email,phone,name,contacts_per_site,req_contacts_ind)

        if len(contact_search)>0:
            # filter out companies we dont need
            contact_search = contact_search[contact_search.company.apply(lambda x: x['id'] not in exclude_company_ids)]
            if len(contact_search)>0:
                contacts_filtered_titles=run_title_match(contact_search,titles)
                contacts_filtered_titles = contacts_filtered_titles[contacts_filtered_titles['hasEmail'] | contacts_filtered_titles['hasMobilePhone']]
                # append older lower rank contacts if their company is not excluded

                # get company id for contact

                if len(contacts_filtered_titles)>0:
                    contacts_filtered_titles['companyid'] = contacts_filtered_titles.company.apply(lambda x: x['id'])
                    contacts_filtered_titles = pd.concat([contacts_filtered_titles, company_contacts_not_chosen]) if len(
                        company_contacts_not_chosen) > 0 else contacts_filtered_titles
                    # get contacts ranked by
                    contacts_filtered_titles['rank'] = contacts_filtered_titles.groupby('companyid')['titlematchscore'].cumcount(ascending=False)+1
                    temp=contacts_filtered_titles[contacts_filtered_titles['rank'] == 1].head(req_contacts_ind-num_contacts)
                    company_contacts_not_chosen=pd.concat([company_contacts_not_chosen,contacts_filtered_titles[~contacts_filtered_titles.id.isin(temp.id.unique())]]) if len(company_contacts_not_chosen)>0 else contacts_filtered_titles[~contacts_filtered_titles.id.isin(temp.id.unique())];
                    contacts_filtered_titles = temp#contacts_filtered_titles[contacts_filtered_titles['rank'] == 1]
                    #contacts_filtered_titles=contacts_filtered_titles
                    enriched_contacts = get_contact_data_enrich_api(contacts_filtered_titles.id.values)
                    if len(enriched_contacts)>0:
                        contacts_filtered_customers=run_customer_matchback(enriched_contacts)
                        if len(contacts_filtered_customers) > 0:
                            contacts_filtered_propects=run_prospect_matchback(contacts_filtered_customers)
                            if len(contacts_filtered_propects) > 0:
                                contacts_filtered = contacts_filtered_propects \
                                if len(contacts_filtered) == 0 else \
                                pd.concat([contacts_filtered_propects, contacts_filtered]) \
                                if len(contacts_filtered_propects) > 0 else contacts_filtered_propects
                                # add companies for whom we  found contacts to the exclusion list
                                exclude_company_ids=contacts_filtered.companyid.unique()
                                num_contacts+=len(contacts_filtered)
                                #remove lower rank contacts whose companies are excluded
                                company_contacts_not_chosen=company_contacts_not_chosen[company_contacts_not_chosen.companyid.apply(lambda x: x not in exclude_company_ids)]
            else:
                break
        else:
            break
        page=page+1
        loops+=1
    return contacts_filtered


def get_industry_intent_contacts(industry, req_contacts_ind, titles, zipcode, contacts_per_site=1):
    num_contacts = 0
    num_loops = 2
    loops = 0
    page = 0
    company_contacts_not_chosen = []
    exclude_company_ids = []
    contacts_filtered_propects = []
    contacts_filtered=[]
    intent_companies=[];
    companies_not_chosen = []
    # loginstatus = get_zoominfo_authentication();
    # login_key = json.loads(loginstatus.text)['jwt']
    while page < 5:
        intent_companies_temp = (get_intent_data_search_api(page, 50, zipcode, industry, None, retry=1))
        intent_companies = pd.concat([intent_companies, intent_companies_temp]) if len(intent_companies) > 0 and len(intent_companies_temp) > 0 \
        else intent_companies if len(intent_companies_temp) == 0 else intent_companies_temp
        page=page+1
    page=0

    if len(intent_companies) > 0:
        while num_contacts <= req_contacts_ind and loops <= num_loops:

                intent_companies['companyid']=intent_companies.company.apply(lambda x: x['id'])
                #print(len(intent_companies['companyid'].unique()))
                contact_search=[]
                for companyid in intent_companies['companyid'].unique() :
                    if companyid not in exclude_company_ids:
                        contact_search_temp = get_contact_data_search_companyid_api(page,50,zipcode,industry,str(companyid),contacts_per_site,req_contacts_ind,20,retry=1)
                        contact_search=pd.concat([contact_search, contact_search_temp]) if len(contact_search) > 0 and len(
                            contact_search_temp) > 0 \
                            else contact_search if len(contact_search_temp) == 0 else contact_search_temp
                if len(contact_search) > 0:
                    # filter out companies we dont need
                    contact_search = contact_search[contact_search.company.apply(lambda x: x['id'] not in exclude_company_ids)]
                    if len(contact_search) > 0:
                        contacts_filtered_titles = run_title_match(contact_search, titles)
                        contacts_filtered_titles = contacts_filtered_titles[
                            contacts_filtered_titles['hasEmail'] | contacts_filtered_titles['hasMobilePhone']]
                        # append older lower rank contacts if their company is not excluded

                        # get company id for contact

                        if len(contacts_filtered_titles) > 0:
                            contacts_filtered_titles['companyid'] = contacts_filtered_titles.company.apply(lambda x: x['id'])
                            contacts_filtered_titles = pd.concat(
                                [contacts_filtered_titles, company_contacts_not_chosen]) if len(
                                company_contacts_not_chosen) > 0 else contacts_filtered_titles
                            # get contacts ranked by
                            contacts_filtered_titles['rank'] = contacts_filtered_titles.groupby('companyid')[\
                                                                   'titlematchscore'].cumcount(ascending=False) + 1

                            temp = contacts_filtered_titles[contacts_filtered_titles['rank'] == 1].head(\
                                req_contacts_ind - num_contacts)
                            company_contacts_not_chosen = pd.concat([company_contacts_not_chosen, \
                                                                     contacts_filtered_titles[\
                                                                         ~contacts_filtered_titles.id.isin(\
                                                                             temp.id.unique())]]) if len(\
                                company_contacts_not_chosen) > 0 else contacts_filtered_titles[\
                                ~contacts_filtered_titles.id.isin(temp.id.unique())];
                            contacts_filtered_titles = temp  # contacts_filtered_titles[contacts_filtered_titles['rank'] == 1]
                            # contacts_filtered_titles=contacts_filtered_titles
                            enriched_contacts = get_contact_data_enrich_api(contacts_filtered_titles.id.values)
                            if len(enriched_contacts)>0:
                                contacts_filtered_customers = run_customer_matchback(enriched_contacts)
                                if len(contacts_filtered_customers) > 0:
                                    contacts_filtered_propects = run_prospect_matchback(contacts_filtered_customers)
                                    if len(contacts_filtered_propects) > 0:
                                        contacts_filtered = contacts_filtered_propects \
                                            if len(contacts_filtered) == 0 else \
                                            pd.concat([contacts_filtered_propects, contacts_filtered]) \
                                                if len(contacts_filtered_propects) > 0 else contacts_filtered_propects
                                        # add companies for whom we  found contacts to the exclusion list
                                        contacts_filtered['companyid'] =contacts_filtered.company.apply(lambda x: re.findall( r'(?<=\'id\':).+?(?=\,)',str(x))[0])
                                        exclude_company_ids = contacts_filtered.companyid.unique()
                                        num_contacts += len(contacts_filtered)
                                        # remove lower rank contacts whose companies are excluded
                                        company_contacts_not_chosen = company_contacts_not_chosen[company_contacts_not_chosen.companyid.apply(\
                                                lambda x: x not in exclude_company_ids)]

                    else:
                        break
                else:
                    break
                page = page + 1
                loops += 1


    return contacts_filtered


def get_required_contacts_branch(branchnum):
    am_count = get_branch_am_count(branchnum);
    max_am_count = get_all_am_count();

    return (am_count/max_am_count)*max_contacts_month


def get_branch_contacts(branchnum,ind_titles_ratio,pulltype):
    required_contacts = get_required_contacts_branch(branchnum)
    #
    zipcode = get_branch_zipcode(branchnum)
    num_filtered_contacts=0
    out=[]
    for index,i in ind_titles_ratio.iterrows():
        req_contacts_ind = int(i.Percentages*required_contacts/100)
        retrieved_contacts=[]
        if req_contacts_ind > 0:
            if pulltype == 'MHA':
                retrieved_contacts=get_industry_mha_contacts(i.zoominfocodes,req_contacts_ind,i.Titles,zipcode,i['Contacts per site'])
            else:
                retrieved_contacts = get_industry_intent_contacts(i.zoominfocodes, req_contacts_ind, i.Titles, zipcode,i['Contacts per site'])
        print(str(branchnum) +' '+pulltype+' \n'+i.zoominfocodes + ' \n'+ str(req_contacts_ind) + ' '+ str(len(retrieved_contacts)))
        if len(retrieved_contacts) > 0:
            out.append(retrieved_contacts)
    return out
    pass

def get_region_contacts(region,pulltype):
    ind_titles_ratio = get_region_titles(region)
    branches = get_branches_in_region(region)
    out=[]
    for br in branches:
        contacts=get_branch_contacts(br,ind_titles_ratio,pulltype)
        if len(contacts)>0:
            out.append(contacts)
    return out
    pass

def get_region_am_contacts(region,pulltype,contactlist):
    #ind_titles_ratio = get_region_titles(region)

    out=[]
    for index,row in contactlist.iterrows():
        #radius,zipcode,industry,company_name,titles,email,phone,name,req_contacts_ind,contacts_per_site=1
        contacts=get_industry_amlist_contacts(row['radius'],row['zipcode'],row['phone,name'],row['industry'],row['company_name'],row['titles'],row['email'],row['phone'],row['name'],1,1)
        if len(contacts)>0:
            out.append(contacts)
    return out
    pass



def get_region_titles(regionnum):
    return region_metadata_file[region_metadata_file.Regnum==regionnum][['Industry','zoominfocodes','Titles','Percentages','Contacts per site']].copy(deep=True)
    pass

def get_contacts():
    out=[]
    for i in region_num_list:
        region_metadata = get_region_metadata(i)
        contacts=get_region_contacts(i,region_metadata.pulltype.unique()[0])
        if len(contacts)>0:
            out.append(contacts)


    return out
    pass

def get_AM_contacts():
    # to be implemented haven`t recieved lists from AMs
    out=[]
    for i in region_num_list:
        region_metadata = get_region_metadata(i)
        contacts=get_region_am_contacts(i)
        if len(contacts)>0:
            out.append(contacts)
    return out
    pass

def write_contacts_marketo(contacts):
    pass

#  main section of the file , all call start here
bypass_matchback=False;
loginstatus = get_zoominfo_authentication();
max_contacts_month = 2000 
region_metadata_file = pd.read_csv('')
region_num_list = region_metadata_file.Regnum.unique(); # regions in metadata file
branch_metadata = pd.read_csv('');
#login_key
if loginstatus.status_code == 200:
    print("Login Succesful !!");
    login_key=json.loads(loginstatus.text)['jwt']
    #start pulling contacts
    contacts=get_contacts();
    if len(contacts)>0:
        df = pd.DataFrame.from_records(contacts[0][0][0])
        df.to_csv('') # name and save output to CSV
    # write a method to write contacts to Marketo
    write_contacts_marketo(contacts)
else:
    print('\n Error in login :' + loginstatus.text);


