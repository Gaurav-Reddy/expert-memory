import pymongo
import numpy as np
import matplotlib.pyplot as plt
import pandas
from pandas.io.json import json_normalize
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["movie"]

def search_production():
    choice=int(input("Enter 1 if you want to search through cast or 2 for crew name"))
    if(choice==1):
        name=input("enter the name of the cast member: \n")
        mycol=mydb['movie_reviews']
        holder="cast.name"
        myquery={holder:name}
        mydoc=mycol.find(myquery,{'title':1,'cast':1})
        pist=list(mydoc)
        df=pandas.DataFrame(pist,columns=['title','cast'])
        m=[]
        for i in df['cast']:
            print(i,end="\n\n")
            m.append(json_normalize(i))
        mycol=mydb['searches']
        myquery={'cast':name}
        mycol.insert_one(myquery)
        print("PASS logged in searches db")
    elif(choice==2):
        name=input("enter the name of the cast member: \n")
        mycol=mydb['movie_reviews']
        holder="crew.name"
        myquery={holder:name}
        mydoc=mycol.find(myquery,{'title':1,'crew':1})
        pist=list(mydoc)
        df=pandas.DataFrame(pist,columns=['title','crew'])
        m=[]
        for i in df['cast']:
            print(i,end="\n\n")
            m.append(json_normalize(i))
        mycol=mydb['searches']
        myquery={'crew':name}
        mycol.insert_one(myquery)
        print("PASS logged in searches db")


def normal_search():
    try:
        print("Here is an example of how to enter the input \n runtime = 100 \n budget > 10000 \n title = tangled \n title = tangled batman")
        stringer=input("Enter the details of the field [Please enter the correct spelling]: \n")
        array=stringer.split()
        #print(array)
        col_name=array.pop(0)
        print(col_name)
        comparator_temp= array.pop(0)
        print(col_name+"vv"+comparator_temp)
        if(comparator_temp=='='):
            comparator='$in'
        elif(comparator_temp=='>'):
            comparator='$gt'
        elif(comparator_temp == ('>=' or '=>')):
            comparator_temp='$gte'
        elif(comparator_temp=='<'):
            comparator_temp='$lt'
        elif(comparator_temp == ('<=' or '=<')):
            comparator_temp='$lte'
        elif(comparator_temp == '!='):
            comparator='$ne'
        value_array= array
        mycol = mydb["movie_two"]
        if(col_name == ('budget'or'popularity'or'revenue'or'runtime'or'vote_average'or'vote_count')):
            if(comparator=='$in'):
                comparator='$eq'
            myquery={col_name:{comparator:float(value_array[0])}}
        elif(col_name ==('keywords')):
            temp=str(col_name+'.name')
            myquery={temp:{'$in':value_array}}
        elif(col_name =='genres'):
            temp=str(col_name+'.name')
            myquery={temp:{'$in':value_array}}
        else:
            myquery={col_name:{comparator:value_array}}
        
        print(myquery)
        mydoc = mycol.find(myquery)
        for x in mydoc:
            print('\n'+'Title: '+x['title'])
            print('Vote average: '+str(x['vote_average']))
            print('Vote count no: '+str(x['vote_count']))
            print('Revenue: '+str(x['revenue']))
            print('Tagline: '+str(x['tagline']))
            print('Overview: '+str(x['overview']))
            print('\nGenres: ')
            for i in x['genres']:
                print(i.get("name"), end =", ")
            print('\n\nKeywords: ')
            for i in x['keywords']:
                print(i.get("name"),end =", ")
        #this is to log most searched attribute 
        mycol=mydb['searches']
        query={col_name:value_array}
        print(query)
        x=mycol.insert_one(query)
        print("PASS logged in searches db")
  
        #
    except:
        print("\n There has been some error!!! The input you gave is not compatible\n")
    
def insert_movie_data():
    try:
        mycol = mydb["movie_data"]
        main_array=[]
        a=1
        while(a==1):
            title=input("Enter the title of the movie: \n")
            budget=input("Enter the budget of the movie: \n")
            revenue=input("Enter the revenue of the movie: \n")
            overview=input("Enter the overview of the movie: \n")
            tagline=input("Enter the tagline of the movie: \n")
            runtime=input("Enter the runtime of the movie: \n")
            language=input("Enter the language of the movie: \n")   
            vote_average=input("Enter the vote average of the movie: \n")
            vote_count=input("Enter the vote count of the movie: \n")
            keywords=input("Enter the keywords with , comma in between them: \n")
            keywords=keywords.split(',')
            key=[]
            gen=[]
            for i in keywords:
                key.append({'name':i})
            genres=input("Enter the genres with , comma in between them: \n")
            genres=genres.split(',')
            for i in genres:
                gen.append({'name':i})
            temp={'title':title,'budget':int(budget),'revenue':int(revenue),'overview':overview,
              'tagline':tagline,'runtime':int(runtime),'language':language,'vote average':float(vote_average),
              'vote count':vote_count,'keywords':key,'genres':gen}
            main_array.append(temp)
            choice3=input("If you want to enter more movie press 1 or press and other number\n:")
            if(choice3 != 1):
                a=0
            x=mycol.insert_many(main_array)
            print(x.inserted_ids)
    except:
        print("\n\nThere has been some error!!! Please check the input you have given\n\n")




def most_common_hits():
    uniq=[]
    mycol = mydb["movie_two"]
    pipeline=[{'$project':{'name':"$title",'id':'$id','total':{'$subtract':["$revenue","$budget"]}}},{'$sort':{'total':-1}},{'$limit':50}]
    mydoc = mycol.aggregate(pipeline)
    id_maindb=[]
    for i in mydoc:
        #print(i)
        id_maindb.append(i['id'])
    mycol = mydb["movie_reviews"]
    myquery1={'movie_id':{'$in':id_maindb}}
    mydoc1 = mycol.find(myquery1)
    choice=input("Enter 1 for cast and 2 for crew")
    
    if(choice=='1'):
        temper='cast'
    else:
        temper='crew'
    for i in mydoc1:
        op=i[temper]
        for i in op:
            uniq.append(i['name'])
    x = np.array(uniq)
    
    
    unique, counts = np.unique(x, return_counts=True)
    tempo = (dict(zip(unique, counts)))
    print(tempo,end="\n")
    return tempo
def most_searched():
    try:
        mycol=mydb['searches']
        mydoc = mycol.find({},{'_id':0})
        pist=list(mydoc)
        idx=pandas.Index(pist)
        print(idx.value_counts())
        
    except:
        print("error unable to get data")
def director_genres_relation():
    mycol=mydb['movie_reviews']
    var1=input("Enter the crew name ie direector or music cordinator etc\n: ")
    var2=input("Enter the actors name etc\n: ")
    query={'$and':[{'crew.name':var1},{'cast.name':var2}]}
    mydoc = mycol.find(query,{'title':1})
    title_list=[]
    for i in mydoc:
        title_list.append(i['title'])
    title_list=["Pirates of the Caribbean: At World's End",
                "Pirates of the Caribbean: Dead Man's Chest",
                'Pirates of the Caribbean: On Stranger Tides', 'Alice in Wonderland',
                'Pirates of the Caribbean: The Curse of the Black Pearl']
    mycol=mydb['movie_two']
    genres_choice=input("Enter the genres name :")
    '''pipeline=[
            {'genres.name':genres_choice},
            {'title':{'$in':title_list}},
            {'$subtract':["$revenue","$budget"]}]'''
    
    pipeline=[ { '$match' : { 'title' :{'$in':title_list} } },
              { '$match' : { 'genres.name' : genres_choice} },
            {'$project':{
                    'title':1,'Profit':{
                            '$subtract':['$revenue','$budget']}}},
                    { '$sort' : { 'Profit': -1 } }
                    
            
             ]        
    mydoc = mycol.aggregate(pipeline)
    for i in mydoc:
        print(i)
    
def relation_graphs_1():
    try:
        
        mycol = mydb["movie_two"]
        pipeline=[  {
                  "$unwind":"$genres"
               },
               {
                "$group": {
                  "_id": "$genres.name",
                   "Total Budgets": {
                      "$sum":"$budget"
                  },"Total revenue": {
                      "$sum":"$revenue"
                  }
               }
            } ]
        name=[]
        budgets=[]
        revenue=[]
        mydoc = mycol.aggregate(pipeline)
        for i in mydoc:
            name.append(i['_id'])
            budgets.append(i['Total Budgets'])
            revenue.append(i['Total revenue'])
        print(name,budgets,revenue)
        barWidth = 0.03

        # The x position of bars
        r1 = np.arange(len(budgets))
        r2 = [x + barWidth for x in r1]
         
        # Create blue bars
        plt.bar(r1, budgets, width = barWidth, color = 'blue', edgecolor = 'black',  capsize=7, label='budget')
         
        # Create cyan bars
        plt.bar(r2, revenue, width = barWidth, color = 'cyan', edgecolor = 'black',  capsize=7, label='revenue')
         
        # general layout
        plt.xticks([r + barWidth for r in range(len(budgets))], name)
        plt.ylabel('Amount')
        plt.legend()
         
        # Show graphic
        plt.show()

    except:
        print("There has been some error kindly check if db is running!!")



        
        
#test
        
if __name__ == "__main__":
    print("We have connected to the database successfully!\nPlease choose the option below\n-> ")
    print("1. Select 1 for searching based on budgets, id ,revenue, title,genres, keywords or production companies ,number of votes or vote average\n")
    print("2. Select 2 if you want to see most searched attribute\n")
    print("3. Select 3 if you want to see the relationship between genres and how revenue and  budget\n")
    print("4. Select 4 if you want to insert new movie data\n")
    print("5. Show the under the director name and genre name which movie was successfull\n")
    print("6. Show the most common hit combination in top 100 blockbusters\n")
    print("7. Select 7 to search the movie through crew or cast")
    choice=int(input())
    
    if(choice==1):
        print("this will be a one field search")
        normal_search()
    elif(choice==4):
        insert_movie_data()
    elif(choice==2):
        most_searched()
    elif(choice==6):
        main_dictionary=most_common_hits()
    elif(choice==3):
        relation_graphs_1();
    elif(choice==5):
        director_genres_relation()
    elif(choice==7):
        search_production()
    else:
        print("Error!")