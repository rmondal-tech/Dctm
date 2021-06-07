package com.dctm.migration;
import com.documentum.fc.client.DfAuthenticationException;
import com.documentum.fc.client.DfClient;
import com.documentum.fc.client.DfIdentityException;
import com.documentum.fc.client.DfPrincipalException;
import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.DfServiceException;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfQueueItem;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.client.IDfUser;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLoginInfo;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfLoginInfo;
import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class Migration {
	static IDfSession idfSession = null;

	public static void main(String[] args) throws DfException {
		// TODO Auto-generated method stub
		//		EXECUTE migrate_content FOR '090xxxxxxxxx70' WITH target_store='centera_store_no_retention',renditions='all',remove_original=TRUE;
		/*		EXECUTE migrate_content WITH target_store='my_target_store',
				renditions='all',remove_original=true,max_migrate_count=1000,
				batch_size=100,sysobject_query=true,type_to_query='my_custom_type',
				query='FOLDER(''/my_cabinet/28'', DESCEND)
				AND a_storage_type=''filestore_01''
				AND r_lock_owner='' '' and r_modify_date>=DATE(''07/15/2011'')
				and r_modify_date<DATE(''07/01/2012'')',
				log_file='D:/tcs_migration/logs/REGIONS/27/TCS_MIGRATION_11082012_100453_REGION_28_01.log'

		 */


		String user = null;
		String passwd = null;
		String docbase = null;

		String dql="select * from dm_user where user_login_name='abcd'";
		
		getDfSession(user, passwd, docbase);


		runQueryAndCraeteLog(dql);


	}

	private static void runQueryAndCraeteLog(String dql) throws DfException {
		IDfQuery query = new DfQuery();
		query.setDQL(dql);
		IDfCollection coll = query.execute(idfSession, 0);

		while ( coll.next() ) {
			IDfTypedObject typeObject = (IDfTypedObject) coll.getTypedObject();
			//System.out.println(“Object Name ” + typeObject.getString(“r_object_id”));
			IDfId id = coll.getId("r_object_id");
			IDfUser user = (IDfUser) idfSession.getObject(id);

			List<String[]> csvData = createCsvLogData(user.getString("r_object_id"),user.getString("user_login_name"));


			writeToCsvFile(csvData);

		}

		if ( coll != null )
			coll.close();
	}

	private static void writeToCsvFile(List<String[]> csvData) {
		try (CSVWriter writer = new CSVWriter(new FileWriter("c:\\test\\test.csv"))) {
			writer.writeAll(csvData);


		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static List<String[]> createCsvLogData(String id, String name) {
		String[] header = {"Object id", "object_name", "is_successfully_migrated", "failure_message"};
		String[] record1 = {id,name, " Yes", ""};
		//     String[] record2 = {"2", "second name", " NO", "dfccxxxxx"};

		List<String[]> list = new ArrayList<>();
		list.add(header);
		list.add(record1);
		//  list.add(record2);

		return list;
	}

	private static void getDfSession(String user, String passwd, String docbase)
			throws DfServiceException, DfIdentityException, DfAuthenticationException, DfPrincipalException {

		IDfSessionManager sessMgr = null;


		IDfLoginInfo login = new DfLoginInfo();
		login.setUser(user);
		login.setPassword(passwd);
		IDfClient client = new DfClient();
		sessMgr = client.newSessionManager();
		sessMgr.setIdentity(docbase, login);
		idfSession = sessMgr.getSession(docbase);

		if ( idfSession != null )
			System.out.println("Session created successfully");
	}




}
