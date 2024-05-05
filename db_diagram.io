Table songs {
  SONG_ID varchar [primary key]    
  SONG_NAME varchar                 
  DURATION_MS number       
  URL varchar                       
  ALBUM_ID varchar                  
  ARTIST_ID varchar                  
}

Table artists {
  ARTIST_ID varchar [primary key]  
  ARTIST_NAME varchar              
  EXTERNAL_URL varchar            
}

Table albums {
  ALBUM_ID varchar [primary key]  
  ALBUM_NAME varchar              
  RELEASE_DATE DATE                
  TOTAL_TRACKS integer             
  URL varchar                       
}

Table weekly_hits {
  id integer [primary key] 
  SONG_ID varchar 
  WEEKLY_RANK number  
  POPULARITY number           
  SONG_ADDED date                   

}

ref : artists.ARTIST_ID < songs.ARTIST_ID
ref : albums.ALBUM_ID < songs.ALBUM_ID
ref : weekly_hits.SONG_ID > songs.SONG_ID
