

# -*- coding: utf-8 -*-
"""
Created on Thu Jul  8 17:04:31 2021

@author: HP
"""

import requests
import pymongo
import time
import datetime

myclient = pymongo.MongoClient("mongodb://localhost:27017/")  # mongoDB localhost

mydb = myclient["db_name"]

#collection = mydb.my_gfg_collection
mycol_group = mydb["group"]
mycol_event = mydb["event"]
mycol_rsvp = mydb["rsvp"]
mycol_member_group=mydb["member"]


auth_token='XXXXXXXXXXXXXXXXXXXXXX' # this is auth_token which I have got from meetup 


proxy_dict={"http":"", "https":"", "ftp":""}

headers = {'Authorization': 'bearer ' + auth_token,
           'Content-Type': 'application/json'
           
           }

def run_query(query): # A simple function to use requests.post to make the API call. Note the json= section.
    tries = 4
    for i in range(tries):
        
        response = requests.post('https://api.meetup.com/gql', json={'query': query}, headers=headers,proxies=proxy_dict)
        if response.status_code == 200:
            return response.json()
        else:
            print(i)
            time.sleep(60)
            continue
    raise Exception("Query failed to run by returning code of {}. {}".format(response.status_code, query))








def query5_func(endcusor_keywordsearch):
# this nwill get the groupurl using endcursor    
    variable2= "art|arts|acting|act|architecture|exhibit|museum|tours|artists|entertainment|creative|drama|drawing|theater|voice|media|music|visual|design|industrial|culture"
    query5="""
{
 keywordSearch(filter:{lat:40.75,lon:-73.98999786376953,query:"%s",radius :35,source:GROUPS}input:{first:1,after:"%s"} )
 {
  count
  pageInfo {
        endCursor
        hasNextPage
      }
  edges{
        node{
            id
            result{
                ... on Group{
                    id
                    urlname
                    link
                    name
                    
                    
                        }
                    
                    }
                }
            }
        }
  
 
 }
""" %(variable2,endcusor_keywordsearch)
    result=run_query(query5)
    return result     
    
# extract the group url 
variable1= "art|arts|acting|act|architecture|exhibit|museum|tours|artists|entertainment|creative|drama|drawing|theater|voice|media|music|visual|design|industrial|culture"
#variable1="[^]*"
query1="""
{
 keywordSearch(filter:{lat:40.75,lon:-73.98999786376953,query:"%s",radius :35,source:GROUPS}input:{first:1} )
 {
  count
  pageInfo {
        endCursor
        hasNextPage
      }
  edges{
        node{
            id
            result{
                ... on Group{
                    id
                    urlname
                    link
                    name
                    
                    
                        }
                    
                    }
                }
            }
        }
  
 
 }
""" %(variable1) 


def query2_func(urlname):
    urlname=urlname        
    query2="""
{
 groupByUrlname(urlname:"%s")
 {
  id
  isPrivate
  
  urlname
  timezone
  zip
  emailListAddress
  needsPhoto
  needsQuestions
  welcomeBlurb
  name
  proJoinDate
  latitude
  longitude
  customMemberLabel
  foundedDate
  description
  city
  state
  country
  joinMode
  
  
  link
  
  topics{
      urlkey
      
      
      id
      name
      }
  
  unifiedEvents{
       pageInfo{
          hasNextPage
          endCursor
          }
      count
      edges{
          node{
              id
              title
              eventUrl
              description
              shortDescription
              status
              
              announceStatus
              dateTime
              duration
              timezone
              endTime
              createdAt
              priceTier
              fees{
                  processingFee{
                      type
                      amount
                      }
                  serviceFee{
                      type
                      amount
                      }
                  tax{
                      type
                      amount
                      }
                  }
              currency
              price
              taxType
              donation{
                  title
                  orgName
                  url
                  }
              maxTickets
              going
              
               
              attendingTicket{
                  id
                  user{
                      name
                      id
                      }
                  event{
                      id
                      }
                  quantity
                  createdAt
                  url
                  status
                  guestsCount
                  }
              
              eventType
              isSaved
              isDeletable
              isOnline
              imageUrl
              
              shortUrl
              
              
              rsvpQuestion
              
              rsvpState
              
              guestsAllowed
              numberOfAllowedGuests
              
              isProEmailShared
              
              
              
              
              
  

  host{
       id
       email
       name
       isAdmin
       lat
       lon
       joinTime
       isNew
       stripeCustomerId
       isLeader
       highestCurrentRole
       hasAcceptedLatestTerms
       recommendedTopics{
           count
           edges{
               cursor
               node{
                   urlkey
                   name
                   id
                   }
               }
           }
       
       }
  group{
        id
        name
        urlname
        link
        
        
        
        }
      venue{
          id
          name
          address
          city
          state
          postalCode
          crossStreet
          country
          neighborhood
          lat
          lng
          zoom
          radius
          }
              
              }
          }
      
      }
  memberships{
      pageInfo{
          hasNextPage
          endCursor
          }
      count
      edges{
          node{
              id
              email 
              name
              isAdmin
              lat
              lon
              joinTime
              subscriptionProfile{
                  plans{
                      id
                      amount
                      adjustedAmount
                      billInterval
                      billIntervalUnit
                      isSavingsPlan
                      renewalCopy
                      orgPlanId
                      
                      description
                      }
                  discount{
                      percentOff
                      duration
                      }
                  
                  
                      
                  }
             isNew
             stripeCustomerId
             isLeader
             highestCurrentRole
             hasAcceptedLatestTerms
             
              
              
              
              
              
              recommendedTopics{
                  edges{
                      node{
                          id
                          name
                          urlkey
                          }
                      }
                  }
              }
          }
      }
  }
 }"""%(urlname) 
    result=run_query(query2)
    return result

 


def query3_func(urlname,endcursor_unifiedEvents):  # here error occured 
# for event of the group, this query will be called only if unifiedEvent  has next =true
    query3="""
{
 groupByUrlname(urlname:"%s")
 {
  unifiedEvents(input:{after:"%s"}){
       pageInfo{
          hasNextPage
          endCursor
          }
      count
      edges{
          node{
              id
              title
              eventUrl
              description
              shortDescription
              status
              
              announceStatus
              dateTime
              duration
              timezone
              endTime
              createdAt
              priceTier
              fees{
                  processingFee{
                      type
                      amount
                      }
                  serviceFee{
                      type
                      amount
                      }
                  tax{
                      type
                      amount
                      }
                  }
              currency
              price
              taxType
              donation{
                  title
                  orgName
                  url
                  }
              maxTickets
              going
              
               
              attendingTicket{
                  id
                  user{
                      name
                      id
                      }
                  event{
                      id
                      }
                  quantity
                  createdAt
                  url
                  status
                  guestsCount
                  }
              
              eventType
              isSaved
              isDeletable
              isOnline
              imageUrl
              
              shortUrl
              
              
              rsvpQuestion
              
              rsvpState
              
              guestsAllowed
              numberOfAllowedGuests
              
              isProEmailShared
              
              
              
              
              
  

  host{
       id
       email
       name
       isAdmin
       lat
       lon
       joinTime
       isNew
       stripeCustomerId
       isLeader
       highestCurrentRole
       hasAcceptedLatestTerms
       recommendedTopics{
           count
           edges{
               cursor
               node{
                   urlkey
                   name
                   id
                   }
               }
           }
       
       }
  group{
        id
        name
        urlname
        link
        
        
        
        }
      venue{
          id
          name
          address
          city
          state
          postalCode
          crossStreet
          country
          neighborhood
          lat
          lng
          zoom
          radius
          }
              
              }
          }
      
      }
  }
 
 
 }"""%(urlname,endcursor_unifiedEvents) 
    result=run_query(query3)
    return result


def query4_func(urlname,endcursor_membership):
# for member of the group, this query will be called only if membership has next =true
    query4="""
{
 groupByUrlname(urlname:"%s")
 {
  memberships(input:{after:"%s"}){
      pageInfo{
          hasNextPage
          endCursor
          }
      count
      edges{
          node{
              id
              email 
              name
              isAdmin
              lat
              lon
              joinTime
              subscriptionProfile{
                  plans{
                      id
                      amount
                      adjustedAmount
                      billInterval
                      billIntervalUnit
                      isSavingsPlan
                      renewalCopy
                      orgPlanId
                      
                      description
                      }
                  discount{
                      percentOff
                      duration
                      }
                  
                  
                      
                  }
             isNew
             stripeCustomerId
             isLeader
             highestCurrentRole
             hasAcceptedLatestTerms
             
              
              
              
              
              
              recommendedTopics{
                  edges{
                      node{
                          id
                          name
                          urlkey
                          }
                      }
                  }
              }
          }
      }
  }
 
 
 }""" %(urlname,endcursor_membership) 
    result=run_query(query4)
    return result

# this quert will be called to find rsvp via query  event where input is the event_id 
def query6_func(event_id):
    urlname=event_id
    query6="""
{
 event(id:"%s")
 {
  eventUrl
  tickets{
      pageInfo{
          hasNextPage
          endCursor
          }
      count
      edges{
          node{
              createdAt
              id
              event{
                  id
                  eventUrl
                  
                  }
              user{
                  hostedEvents{
                      count
                      edges{
                          node{
                              id
                              eventUrl
                              title
                              }
                          }
                      
                      }
                  id
                  email
                  name
                  isAdmin
                  lat
                  lon
                  isNew
                  isLeader
                  stripeCustomerId
                  highestCurrentRole
                  hasAcceptedLatestTerms
                  recommendedTopics{
                      count
                      edges{
                          node{
                              urlkey
                              name
                              id
                              
                              }
                          }
                      }
                  }
              
             quantity
              
              url
              status
              guestsCount
              
              
              }
              }
      
      
      } 
          
  
  }
  }"""%(urlname)
    result=run_query(query6)
    return result



# query to get rsvp using query event having 2 input parameter event_id and rsvp_endcursor
def query7_func(event_id,rsvp_endcursor):
    event_id=event_id
    rsvp_endcursor=rsvp_endcursor
    
    query7="""
{
 event(id:"%s")
 {
  tickets(input:{after:"%s"}){
      pageInfo{
          hasNextPage
          endCursor
          }
      count
      edges{
          node{
              createdAt
              id
              event{
                  id
                  eventUrl
                  
                  }
              user{
                  hostedEvents{
                      count
                      edges{
                          node{
                              id
                              eventUrl
                              title
                              }
                          }
                      
                      }
                  id
                  name
                  isAdmin
                  lat
                  lon
                  isNew
                  isLeader
                  stripeCustomerId
                  highestCurrentRole
                  hasAcceptedLatestTerms
                  recommendedTopics{
                      count
                      edges{
                          node{
                              urlkey
                              name
                              id
                              
                              }
                          }
                      }
                  }
               quantity
              createdAt
              url
              status
              guestsCount
             
              
              
              }
              }
      
      
      } 
          
  
  }
  }"""%(event_id,rsvp_endcursor)
    result=run_query(query7)
    return result

iteration=1
code_end=0
while(1):
    
    
    if(iteration==1): # for 1st iteration
        f= open("arts_log.txt","a")
        result=run_query(query1)
        endCursor_keywordsearch=result['data']['keywordSearch']['pageInfo']['endCursor']
        f.write(endCursor_keywordsearch)
        f.write("\n")
        f.write(str(datetime.datetime.now()))
        f.write("\n")
        f.close()
        
        
    if(iteration==2):
        f= open("arts_log.txt","a")
        result=query5_func(endCursor_keywordsearch)
        endCursor_keywordsearch=result['data']['keywordSearch']['pageInfo']['endCursor']
        f.write(endCursor_keywordsearch)
        f.write("\n")
        f.write(str(datetime.datetime.now()))
        f.write("\n")
        f.close()
        
    if(result['data']['keywordSearch']['pageInfo']['hasNextPage']==False): # checking if no group exists to search
        code_end=1
        break
    group_id= result['data']['keywordSearch']['edges'][0]['node']['result']['id']  # to store group_id required later in member details
    group_name=result['data']['keywordSearch']['edges'][0]['node']['result']['name']
    group_urlname=result['data']['keywordSearch']['edges'][0]['node']['result']['urlname']
    group_link=result['data']['keywordSearch']['edges'][0]['node']['result']['link']
    #checking if hashnextpage is true
    if(result['data']['keywordSearch']['pageInfo']['hasNextPage']==True):
        endCursor_group=result['data']['keywordSearch']['pageInfo']['endCursor']
    else:
        #get out of the while(1) loop
        break
    group_url=result['data']['keywordSearch']['edges'][0]['node']['result']['urlname'] # this group_url is required in passing argument function
    result=query2_func(group_url)
    result1=result  # 2 copies of the result
    result2=result
    if(result['data']['groupByUrlname']['unifiedEvents']['pageInfo']['hasNextPage']==True):
        endCursor_event=result['data']['groupByUrlname']['unifiedEvents']['pageInfo']['endCursor']

    event={}
    rsvp={}

# this loop will iterate for each event of the group 
    for i in range(len(result['data']['groupByUrlname']['unifiedEvents']['edges'])):
        # pop the tickets details from events
        #rsvp=result['data']['groupByUrlname']['unifiedEvents']['edges'][i]['node'].pop('tickets') # many tickets
        event=result['data']['groupByUrlname']['unifiedEvents']['edges'][i]['node']  # store individual event details
        #del(result['data']['groupByUrlname']['unifiedEvents']['edges'][i]['node']['tickets'])
        # check if 
        # insert event in mongodb
        xevent = mycol_event.insert_one(event)
        
        '''if(event['tickets']['pageInfo']['hasNextPage']==True):
             rsvp_endcursor=event['tickets']['pageInfo']['endCursor']'''
        event_id=event['id']  # this event id will be needed for query6 and query7
        
        #event_id=str(220956148)
        # call the event query only takling argument as event_id
        result3=query6_func(event_id)
        times=0
        # result3 will have list of rsvp
        for j in range(len(result3['data']['event']['tickets']['edges'])):
            rsvp=result3['data']['event']['tickets']['edges'][j]
            
            xrsvp = mycol_rsvp.insert_one(rsvp)
            # store the rsvp now in mongo db
        while  (result3['data']['event']['tickets']['pageInfo']['hasNextPage']== True):
            rsvp_endcursor=result3['data']['event']['tickets']['pageInfo']['endCursor']
            result3=query7_func(event_id,rsvp_endcursor)
            #if(result3['data']['event']['tickets']['pageInfo']['hasNextPage']== True):
            for k in range(len(result3['data']['event']['tickets']['edges'])):
                rsvp=result3['data']['event']['tickets']['edges'][k]
                
                xrsvp = mycol_rsvp.insert_one(rsvp)
                # Store rsvp in mongodb
                
            
            
        
             
            
        
                                             
   
        event={}
        rsvp={}
    while(result['data']['groupByUrlname']['unifiedEvents']['pageInfo']['hasNextPage']==True):
        endCursor_event=result['data']['groupByUrlname']['unifiedEvents']['pageInfo']['endCursor']
        result=query3_func(group_url,endCursor_event) # error occured with error 503
        for i in range(len(result['data']['groupByUrlname']['unifiedEvents']['edges'])):
            # pop the tickets details from events
            #rsvp=result['data']['groupByUrlname']['unifiedEvents']['edges'][i]['node'].pop('tickets') # many tickets
            event=result['data']['groupByUrlname']['unifiedEvents']['edges'][i]['node']  # store individual event details
            xevent = mycol_event.insert_one(event)
            event_id=event['id']  # this event id will be needed for query6 and query7
            #dict_event[event_id]=event
            # call the event query only takling argument as event_id
            result3=query6_func(event_id)
            
            # result3 will have list of rsvp
            for j in range(len(result3['data']['event']['tickets']['edges'])):
                rsvp=result3['data']['event']['tickets']['edges'][j]
                
                
                xrsvp = mycol_rsvp.insert_one(rsvp)
            # store the rsvp now in mongo db
            while  (result3['data']['event']['tickets']['pageInfo']['hasNextPage']== True):
                rsvp_endcursor=result3['data']['event']['tickets']['pageInfo']['endCursor']
                result3=query7_func(event_id,rsvp_endcursor)
                for k in range(len(result3['data']['event']['tickets']['edges'])):
                    rsvp=result3['data']['event']['tickets']['edges'][k]
                    
                    xrsvp = mycol_rsvp.insert_one(rsvp)
                    # store the rsvp now in mongodb
            
            
            
            
    result=result2
    # for members of the group
    # commentinf the membership details
    if(result['data']['groupByUrlname']['memberships']['pageInfo']['hasNextPage']==True):
        endCursor_member=result['data']['groupByUrlname']['memberships']['pageInfo']['endCursor']
        
    member_group={}
    i=0     
    for i in range(len(result['data']['groupByUrlname']['memberships']['edges'])):
        member_group=result['data']['groupByUrlname']['memberships']['edges'][i]
        
        member_group['group_id']=group_id
        member_group['group_name']=group_name
        member_group['group_urlname']=group_urlname
        member_group['group_link']=group_link
        
        
        xmember = mycol_member_group.insert_one(member_group) 
    
    # insert member_group dictionary into mongodp
        member_group={}
        
    while(result['data']['groupByUrlname']['memberships']['pageInfo']['hasNextPage']==True):
        endCursor_member=result['data']['groupByUrlname']['memberships']['pageInfo']['endCursor']
        result=query4_func(group_url,endCursor_member)
        m=0
        for m in range(len(result['data']['groupByUrlname']['memberships']['edges'])):
            member_group=result['data']['groupByUrlname']['memberships']['edges'][m]
            member_group['group_id']=group_id
            member_group['group_name']=group_name
            member_group['group_urlname']=group_urlname
            member_group['group_link']=group_link
            
            xmember = mycol_member_group.insert_one(member_group) 
        # insert member_group dictionary into mongodp
            member_group={}
            
    result=result2
    number_of_member=result['data']['groupByUrlname']['memberships']['count']
    number_of_event=result['data']['groupByUrlname']['unifiedEvents']['count']
    result['data']['number of event']=number_of_event
    result['data']['number of member']=number_of_member    
    del(result['data']['groupByUrlname']['memberships'])
    del(result['data']['groupByUrlname']['unifiedEvents'])
    # insert result that is the group details in mongodb
    
    xgroup = mycol_group.insert_one(result['data'])
    print("end of 1st group")
    iteration=2   # update the iteration     

if(code_end==1):
    print("All groups fetched of the particular query")
print("End")