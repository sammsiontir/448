package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

public class MyFakebookOracle extends FakebookOracle {
	
	static String prefix = "singhmk.";
	
	// You must use the following variable as the JDBC connection
	Connection oracleConnection = null;
	
	// You must refer to the following variables for the corresponding tables in your database
	String cityTableName = null;
	String userTableName = null;
	String friendsTableName = null;
	String currentCityTableName = null;
	String hometownCityTableName = null;
	String programTableName = null;
	String educationTableName = null;
	String eventTableName = null;
	String participantTableName = null;
	String albumTableName = null;
	String photoTableName = null;
	String coverPhotoTableName = null;
	String tagTableName = null;
	
	
	// DO NOT modify this constructor
	public MyFakebookOracle(String u, Connection c) {
		super();
		String dataType = u;
		oracleConnection = c;
		// You will use the following tables in your Java code
		cityTableName = prefix+dataType+"_CITIES";
		userTableName = prefix+dataType+"_USERS";
		friendsTableName = prefix+dataType+"_FRIENDS";
		currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
		hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
		programTableName = prefix+dataType+"_PROGRAMS";
		educationTableName = prefix+dataType+"_EDUCATION";
		eventTableName = prefix+dataType+"_USER_EVENTS";
		albumTableName = prefix+dataType+"_ALBUMS";
		photoTableName = prefix+dataType+"_PHOTOS";
		tagTableName = prefix+dataType+"_TAGS";
	}
	
	
	@Override
	// ***** Query 0 *****
	// This query is given to your for free;
	// You can use it as an example to help you write your own code
	//
	public void findMonthOfBirthInfo() throws SQLException{ 
		
		// Scrollable result set allows us to read forward (using next())
		// and also backward.  
		// This is needed here to support the user of isFirst() and isLast() methods,
		// but in many cases you will not need it.
		// To create a "normal" (unscrollable) statement, you would simply call
		// Statement stmt = oracleConnection.createStatement();
		//
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		        ResultSet.CONCUR_READ_ONLY);
		
		// For each month, find the number of friends born that month
		// Sort them in descending order of count
		ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from "+
				userTableName+
				" where month_of_birth is not null group by month_of_birth order by 1 desc");
		
		this.monthOfMostFriend = 0;
		this.monthOfLeastFriend = 0;
		this.totalFriendsWithMonthOfBirth = 0;
		
		// Get the month with most friends, and the month with least friends.
		// (Notice that this only considers months for which the number of friends is > 0)
		// Also, count how many total friends have listed month of birth (i.e., month_of_birth not null)
		//
		while(rst.next()) {
			int count = rst.getInt(1);
			int month = rst.getInt(2);
			if (rst.isFirst())
				this.monthOfMostFriend = month;
			if (rst.isLast())
				this.monthOfLeastFriend = month;
			this.totalFriendsWithMonthOfBirth += count;
		}
		
		// Get the names of friends born in the "most" month
		rst = stmt.executeQuery("select user_id, first_name, last_name from "+
				userTableName+" where month_of_birth="+this.monthOfMostFriend);
		while(rst.next()) {
			Long uid = rst.getLong(1);
			String firstName = rst.getString(2);
			String lastName = rst.getString(3);
			this.friendsInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Get the names of friends born in the "least" month
		rst = stmt.executeQuery("select first_name, last_name, user_id from "+
				userTableName+" where month_of_birth="+this.monthOfLeastFriend);
		while(rst.next()){
			String firstName = rst.getString(1);
			String lastName = rst.getString(2);
			Long uid = rst.getLong(3);
			this.friendsInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Close statement and result set
		rst.close();
		stmt.close();
	}

	
	
	@Override
	// ***** Query 1 *****
	// Find information about friend names:
	// (1) The longest last name (if there is a tie, include all in result)
	// (2) The shortest last name (if there is a tie, include all in result)
	// (3) The most common last name, and the number of times it appears (if there is a tie, include all in result)
	//
	public void findNameInfo() throws SQLException { // Query1
		// Find the following information from your database and store the information as shown 
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		        ResultSet.CONCUR_READ_ONLY);
		ResultSet rst = stmt.executeQuery("select count(*), last_name from "+
				userTableName+
				" where last_name is not null group by last_name order by 1 desc");
		this.mostCommonLastNamesCount = 0;
		int topCommonLastNamesCount = 0;
		int longestLastNamesLength = 0;
		int shortestLastNamesLength = 1000;
		while(rst.next()) {
			int count = rst.getInt(1);
			String last_name = rst.getString(2);
			if (rst.isFirst()){
				topCommonLastNamesCount = count;
			}
			if(count == topCommonLastNamesCount){
				this.mostCommonLastNames.add(last_name);
				this.mostCommonLastNamesCount += count;				
			}
			if(last_name.length()>longestLastNamesLength){
				longestLastNames.clear();
				longestLastNamesLength = last_name.length();
			}			
			else if(last_name.length()<shortestLastNamesLength){
				shortestLastNames.clear();
				shortestLastNamesLength = last_name.length();
			}
			if(last_name.length()==longestLastNamesLength){
				longestLastNames.add(last_name);
			}
			else if(last_name.length()==shortestLastNamesLength){
				shortestLastNames.add(last_name);
			}			
		}
		rst.close();
		stmt.close();
	}
	
	@Override
	// ***** Query 2 *****
	// Find the user(s) who have no friends in the network
	//
	// Be careful on this query!
	// Remember that if two users are friends, the friends table
	// only contains the pair of user ids once, subject to 
	// the constraint that user1_id < user2_id
	//
	public void lonelyFriends() throws SQLException {
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
			ResultSet.CONCUR_READ_ONLY);
		ResultSet rst = stmt.executeQuery("select u.user_id from "+userTableName+
				" u where u.user_id not in ( select distinct F1.user1_id as user_id from "+
				friendsTableName+" F1 union select distinct F2.user2_id as user_id from "+
				friendsTableName+" F2)");
		this.countLonelyFriends = 0;
		while (rst.next()) {
                        this.lonelyFriends.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
                        this.countLonelyFriends++;        
                }
                
                // Close result set
                rst.close();
                stmt.close();
	}
	 

	@Override
	// ***** Query 3 *****
	// Find the users who still live in their hometowns
	// (I.e., current_city = hometown_city)
	//	
	public void liveAtHome() throws SQLException {
		Statement stmt = oracleConnection.createStatement();

		ResultSet rst = stmt.executeQuery(
				"select U.user_id, U.first_name, U.last_name from "+
				userTableName+" U, "+currentCityTableName+" CC, "+hometownCityTableName+" HC"+
				" where U.user_id = CC.user_id AND U.user_id = HC.user_id AND CC.current_city_id = HC.hometown_city_id"
		); 

		this.countLiveAtHome = 0; 

		while(rst.next()) {
			Long uid = rst.getLong(1);
			String firstName = rst.getString(2);
			String lastName = rst.getString(3);
			this.liveAtHome.add(new UserInfo(uid, firstName, lastName));
			this.countLiveAtHome++;
		} 

		rst.close();
		stmt.close();
	}



	@Override
	// **** Query 4 ****
	// Find the top-n photos based on the number of tagged users
	// If there are ties, choose the photo with the smaller numeric PhotoID first
	// 
	public void findPhotosWithMostTags(int n) throws SQLException { 
		Statement stmt1 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		                                                  ResultSet.CONCUR_READ_ONLY);
                Statement stmt2 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
	                                                          ResultSet.CONCUR_READ_ONLY);

		// For each month, find the number of friends born that month
                ResultSet rst1 = stmt1.executeQuery(
                 	       "select P.PHOTO_ID, P.ALBUM_ID, A.ALBUM_NAME, P.PHOTO_CAPTION,"+
				" P.PHOTO_LINK from (select count(*) as num, T.TAG_PHOTO_ID as photo_id from "
				+tagTableName+ " T "+
				" where T.TAG_SUBJECT_ID is not null group by T.TAG_PHOTO_ID) G, "
				+photoTableName+" P, "+albumTableName+
				" A where P.album_id = A.album_id and P.photo_id = G.photo_id"+
				" order by G.num desc, P.PHOTO_ID asc");

                int i = 0;

                while( rst1.next() && i < n ){
                        TaggedPhotoInfo tp = new TaggedPhotoInfo(
                                             new PhotoInfo(rst1.getString(1),
							   rst1.getString(2),
							   rst1.getString(3),
                                                           rst1.getString(4),
							   rst1.getString(5)));
                        ResultSet rst2 = stmt2.executeQuery(
				"select U.user_id, U.first_name, U.last_name from "
				+userTableName+" U, "+tagTableName+
				" T where T.tag_photo_id = "+rst1.getString(1)+" and U.user_id = T.tag_subject_id");
                        
                        while (rst2.next()){
                                 long uid = rst2.getLong(1);
                                 String firstName = rst2.getString(2);
                                 String lastName = rst2.getString(3);
                                 tp.addTaggedUser(new UserInfo(uid, firstName, lastName));
                        }
                        this.photosWithMostTags.add(tp);
                        rst2.close();
                        i++;
                }
                
                // Close result set
                rst1.close();
                stmt1.close();
	}

	
	
	
	@Override
	// **** Query 5 ****
	// Find suggested "match pairs" of friends, using the following criteria:
	// (1) One of the friends is female, and the other is male
	// (2) Their age difference is within "yearDiff"
	// (3) They are not friends with one another
	// (4) They should be tagged together in at least one photo
	//
	// You should up to n "match pairs"
	// If there are more than n match pairs, you should break ties as follows:
	// (i) First choose the pairs with the largest number of shared photos
	// (ii) If there are still ties, choose the pair with the smaller user_id for the female
	// (iii) If there are still ties, choose the pair with the smaller user_id for the male
	//
	public void matchMaker(int n, int yearDiff) throws SQLException { 
		Statement stmt = oracleConnection.createStatement();
		ResultSet rst = stmt.executeQuery("select U1.user_id, U1.first_name, U1.last_name, U1.year_of_birth, "+
		"U2.user_id, U2.first_name, U2.last_name, U2.year_of_birth, TP.photo_id, TP.photo_num, P.album_id, P.photo_caption, P.photo_link, A.album_name from "+
		userTableName+" U1, "+userTableName+" U2, "+photoTableName+" P, "+albumTableName+" A, "+
		"(select TT1.tag1_id AS tag1_id, TT1.tag2_id AS tag2_id, TT1.photo_id AS photo_id, PN1.photo_num AS photo_num from "+
		"(select count(*) AS photo_num, T1.tag_photo_id AS photo_id from "+tagTableName+" T1, "+tagTableName+" T2 "+
		"where T1.tag_photo_id = T2.tag_photo_id AND T1.tag_subject_id < T2.tag_subject_id "+
		"group by T1.tag_photo_id) PN1, "+
		"(select distinct T1.tag_subject_id AS tag1_id, T1.tag_photo_id AS photo_id, T2.tag_subject_id AS tag2_id from "+
		tagTableName+" T1, "+tagTableName+" T2 where T1.tag_photo_id = T2.tag_photo_id AND T1.tag_subject_id < T2.tag_subject_id) TT1 "+
		"where PN1.photo_id = TT1.photo_id minus "+
		"select TT2.tag1_id AS tag1_id, TT2.tag2_id AS tag2_id, TT2.photo_id AS photo_id, PN1.photo_num AS photo_num from "+
		friendsTableName+" F, "+
		"(select count(*) AS photo_num, T1.tag_photo_id AS photo_id from "+tagTableName+" T1, "+tagTableName+" T2 "+
		"where T1.tag_photo_id = T2.tag_photo_id AND T1.tag_subject_id < T2.tag_subject_id group by T1.tag_photo_id) PN1, "+
		"(select distinct T1.tag_subject_id AS tag1_id, T1.tag_photo_id AS photo_id, T2.tag_subject_id AS tag2_id from "+
		tagTableName+" T1, "+tagTableName+" T2 where T1.tag_photo_id = T2.tag_photo_id AND T1.tag_subject_id < T2.tag_subject_id) TT2 "+
		"where TT2.tag1_id = F.user1_id AND TT2.tag2_id = F.user2_id AND PN1.photo_id = TT2.photo_id) TP "+
		"where ((TP.tag1_id = U1.user_id AND TP.tag2_id = U2.user_id AND U1.gender = 'female' AND U2.gender = 'male' "+
		"AND U1.year_of_birth-U2.year_of_birth <="+yearDiff+" AND U1.year_of_birth-U2.year_of_birth>=-"+yearDiff+") OR "+
		"(TP.tag1_id = U2.user_id AND TP.tag2_id = U1.user_id AND U1.gender = 'female' AND U2.gender = 'male' "+
		"AND U1.year_of_birth-U2.year_of_birth <="+yearDiff+" AND U1.year_of_birth-U2.year_of_birth>=-"+yearDiff+")) AND TP.photo_id = P.photo_id AND P.album_id = A.album_id "+
		"order by TP.photo_num DESC, U1.user_id ASC, U2.user_id ASC");	
		while(rst.next()){
			Long girlUserId = rst.getLong(1);
			String girlFirstName = rst.getString(2);
			String girlLastName = rst.getString(3);
			int girlYear = rst.getInt(4);
			Long boyUserId = rst.getLong(5);
			String boyFirstName = rst.getString(6);
			String boyLastName = rst.getString(7);
			int boyYear = rst.getInt(8);		
			MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName, 
				girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
			String sharedPhotoId = rst.getString(9);
			String sharedPhotoAlbumId = rst.getString(11);
			String sharedPhotoAlbumName = rst.getString(14);
			String sharedPhotoCaption = rst.getString(12);
			String sharedPhotoLink = rst.getString(13);		
			mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
				sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
			this.bestMatches.add(mp);
			n--;
			if(n==0)
				break;
		}
		rst.close();
		stmt.close();
	}

	
	
	// **** Query 6 ****
	// Suggest friends based on mutual friends
	// 
	// Find the top n pairs of users in the database who share the most
	// friends, but such that the two users are not friends themselves.
	//
	// Your output will consist of a set of pairs (user1_id, user2_id)
	// No pair should appear in the result twice; you should always order the pairs so that
	// user1_id < user2_id
	//
	// If there are ties, you should give priority to the pair with the smaller user1_id.
	// If there are still ties, give priority to the pair with the smaller user2_id.
	//
	@Override
	public void suggestFriendsByMutualFriends(int n) throws SQLException {
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		        ResultSet.CONCUR_READ_ONLY);
		ResultSet rst = stmt.executeQuery("select GP.mfuser1_id, GP.mfuser2_id, GP.count from "+
				"( select MF.user1_id AS mfuser1_id, MF.user2_id AS mfuser2_id, count(*) AS count "+
				"from ( select distinct NF.user1_id AS user1_id, NF.user2_id AS user2_id, U.user_id AS mf_id "+
				"from "+friendsTableName+" T1, "+friendsTableName+" T2, "+userTableName+" U, "+
				"(select U1.user_id AS user1_id, U2.user_id AS user2_id from "+userTableName+"  U1, "+userTableName+" U2 "+
				"where U1.user_id < U2.user_id minus "+
				"select U1.user_id AS user1_id, U2.user_id AS user2_id from "+
				userTableName+" U1, "+userTableName+" U2, "+friendsTableName+" F "+
				"where U1.user_id < U2.user_id AND U1.user_id = F.user1_id AND U2.user_id = F.user2_id) NF "+
				"where (T1.user1_id = U.user_id AND T1.user1_id = T2.user1_id AND NF.user1_id = T1.user2_id AND NF.user2_id = T2.user2_id) "+
				"OR (T1.user2_id = U.user_id AND T1.user2_id = T2.user1_id AND NF.user1_id = T1.user1_id AND NF.user2_id = T2.user2_id) "+
				"OR (T1.user2_id = U.user_id AND T1.user2_id = T2.user2_id AND NF.user1_id = T1.user1_id AND NF.user2_id = T2.user1_id))MF "+
				"group by MF.user1_id, MF.user2_id order by count(*) DESC, MF.user1_id, MF.user2_id)GP "+
				"where rownum<="+n);
		while(rst.next()){
			int countMutualFriends = rst.getInt(3);
			Long user1_id = rst.getLong(1);
			Long user2_id = rst.getLong(2);
			Statement stmt2 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
			        ResultSet.CONCUR_READ_ONLY);
			ResultSet rst2 = stmt2.executeQuery("select NF.user1_id AS user1_id, NF.user1_first_name AS user1_first_name, "+
				"NF.user1_last_name AS user1_last_name, NF.user2_id AS user2_id, NF.user2_first_name AS user2_first_name, "+
				"NF.user2_last_name AS user2_last_name, U.user_id AS mf_id, U.first_name AS mf_first_name, U.last_name AS mf_last_name from "+
				friendsTableName+" T1, "+friendsTableName+" T2, "+userTableName+" U, "+
				"(select U1.user_id AS user1_id, U1.first_name AS user1_first_name, U1.last_name AS user1_last_name, U2.user_id AS user2_id, "+
				"U2.first_name AS user2_first_name, U2.last_name AS user2_last_name from "+userTableName+" U1, "+userTableName+" U2 "+
				"where U1.user_id = "+user1_id+" AND U2.user_id = "+user2_id+") NF "+
				"where ((T1.user1_id = U.user_id AND T1.user1_id = T2.user1_id AND NF.user1_id = T1.user2_id AND NF.user2_id = T2.user2_id) "+
				"OR (T1.user2_id = U.user_id AND T1.user2_id = T2.user1_id AND NF.user1_id = T1.user1_id AND NF.user2_id = T2.user2_id) "+
				"OR (T1.user2_id = U.user_id AND T1.user2_id = T2.user2_id AND NF.user1_id = T1.user1_id AND NF.user2_id = T2.user1_id)) order by mf_id");		
			rst2.next();
			String user1FirstName = rst2.getString(2);
			String user1LastName = rst2.getString(3);
			String user2FirstName = rst2.getString(5);
			String user2LastName = rst2.getString(6);
			FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
			rst2.previous();
			while(countMutualFriends>0){
				rst2.next();
				p.addSharedFriend(rst2.getLong(7), rst2.getString(8), rst2.getString(9));
				this.suggestedFriendsPairs.add(p);
				countMutualFriends--;
			}
			rst2.close();
			stmt2.close();
		}
		rst.close();
		stmt.close();
	}
	
	
	@Override
	// ***** Query 7 *****
	// Given the ID of a user, find information about that
	// user's oldest friend and youngest friend
	// 
	// If two users have exactly the same age, meaning that they were born
	// on the same day, then assume that the one with the larger user_id is older
	//
	public void findAgeInfo(Long user_id) throws SQLException {
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		        ResultSet.CONCUR_READ_ONLY);
		ResultSet rst = stmt.executeQuery("select F.user1_id AS friend_id, " +
				"U.first_name, U.last_name, U.year_of_birth, U.month_of_birth, U.day_of_birth from "+friendsTableName+" F, "
				+userTableName+" U where F.user2_id = "+user_id+" AND U.user_id = F.user1_id"+
				" UNION "+
				"select F.user2_id AS friend_id, U.first_name, U.last_name, U.year_of_birth, U.month_of_birth, U.day_of_birth from "
				+friendsTableName+" F, "+userTableName+" U where F.user1_id = "+user_id+" AND U.user_id = F.user2_id"+
				" order by 4 ASC, 5 ASC, 6 ASC, friend_id DESC");		
		while(rst.next()) {
			if (rst.isFirst())
				this.oldestFriend = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
			if (rst.isLast())
				this.youngestFriend = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
		}		
		rst.close();
		stmt.close();
	}

	@Override
	// ***** Query 8 *****
	// 
	// Find the name of the city with the most events, as well as the number of 
	// events in that city.  If there is a tie, return the names of all of the (tied) cities.
	//
	public void findEventCities() throws SQLException {
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		                                                  ResultSet.CONCUR_READ_ONLY);
                ResultSet rst = stmt.executeQuery(
                        "select T.num, C.city_name from "
			+cityTableName+
			" C, (select count(*) as num, E.event_city_id as city_id from "
			+eventTableName+
			" E group by event_city_id) T where T.city_id = C.city_id order by T.num desc"
                );
		rst.next();
		int max_val = rst.getInt(1);
		this.eventCount = rst.getInt(1);
		this.popularCityNames.add(rst.getString(2));

		while (rst.next()) {
                        if (max_val > rst.getInt(1)) break;
			this.eventCount = rst.getInt(1);
			this.popularCityNames.add(rst.getString(2));
                }
                // Close result set
                rst.close();
                stmt.close();
	}
	
	
	
	@Override
//	 ***** Query 9 *****
	//
	// Find pairs of potential siblings and print them out in the following format:
	//   # pairs of siblings
	//   sibling1 lastname(id) and sibling2 lastname(id)
	//   siblingA lastname(id) and siblingB lastname(id)  etc.
	//
	// A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
	// if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
	// on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
	//  
	//
	public void findPotentialSiblings() throws SQLException {
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		                                                  ResultSet.CONCUR_READ_ONLY);

                ResultSet rst = stmt.executeQuery(
                        "select U1.user_id, U1.first_name, U1.last_name, "
			+"U2.user_id, U2.first_name, U2.last_name from "
			+userTableName+" U1, "+userTableName+" U2, "
			+hometownCityTableName+" H1, "+hometownCityTableName+" H2 "
			+"where U1.last_name = U2.last_name "
			+"and U1.user_id = H1.user_id and U2.user_id = H2.user_id "
			+"and H1.hometown_city_id = H2.hometown_city_id "
			+"and U1.user_id in (select F.user1_id as user_id from public_friends F where F.user2_id = U2.user_id "
			+"union select F.user2_id as user_id from public_friends F where F.user1_id = U2.user_id) "
			+"and (U1.year_of_birth - U2.year_of_birth) < 10 "
			+"and (U2.year_of_birth - U1.year_of_birth) < 10 "
			+"and U1.user_id < U2.user_id "
			+"order by U1.user_id, U2.user_id"
		);		

		while (rst.next()){
			this.siblings.add(new SiblingInfo(rst.getLong(1), rst.getString(2), rst.getString(3), rst.getLong(4), rst.getString(5), rst.getString(6)));
		}

                // Close result set
                rst.close();
                stmt.close();
	}
	
}
