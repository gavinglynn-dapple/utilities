import {getPrismaClient} from "../src/prisma/Primsa";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import * as fs from "node:fs/promises";
import {FileHandle} from "node:fs/promises";

dayjs.extend(utc);
dayjs.extend(timezone);

const smsGsmCharacters =
	" @ΔSP0¡P¿p£_!1AQaq$Φ\"2BRbr¥Γ#3CScsèΛ¤4DTdtéΩ%5EUeuùΠ&6FVfvìΨ'7GWgwòΣ(8HXhxÇΘ)9IYiy\nΞ*:JZjzØ\f\x1B;KÄkäøÆ,<LÖlö\ræ-=MÑmñÅß.>NÜnüåÉ/?O§oà|^€{}[~]\\";

const escapeCsv = (value: string | undefined | null) =>
	value ? value.replace(/"/g, '""') : "";

const hasNonGsmCharacters = (value: string | undefined | null): boolean => {
	const localValue = value ?? "";
	for (let index = 0; index < localValue.length; ++index) {
		const ch = localValue[index];
		if (!smsGsmCharacters.includes(ch)) {
			//			console.log(
			//				`Value: ${localValue}\nIndex: ${index}\nCharacter: ${ch} (0x${ch.charCodeAt(0).toString(16)})`,
			//			);
			return true;
		}
	}
	return false;
};

const hasBackTickCharacter = (value: string | undefined | null): boolean => {
	const localValue = value ?? "";
	return localValue.includes("\u2019");
};

/*
const prismaClient = await getPrismaClient();

const templates = await prismaClient.marketing_SMSMessages.findMany({});
const filteredTemplates = templates.filter(template =>
	hasBackTickCharacter(template.message),
);
console.log(templates);
console.log(filteredTemplates);

for (const template of filteredTemplates) {
	await prismaClient.marketing_SMSMessages.update({
		where: {
			id_tenant: {
				id: template.id,
				tenant: template.tenant,
			},
		},
		data: {
			message: template.message?.replace(/\u2019/g, "'") ?? null,
		},
	});
}
*/

/*
const customers = await prismaClient.customers.findMany({
	where: {
		tenant: "org_4YBdYHe6FKM0nW20",
		given: "Nicola",
		surname: "Gillies",
	},
});

console.log(customers);
*/

const tenantIds = [
	"org_zjoLqb3Bk8bWIeAx",
	"org_ZHcvdozM0vofm65c",
	"org_SpgN28ViN9vutxVV",
	"org_tQ90wyx3CxYzLqtR",
	"org_RBQMf8HftxQV5Ies",
];

const prismaClient = await getPrismaClient();

const tenants = await prismaClient.tenants.findMany({
	where: {
		id: {
			in: tenantIds,
		},
	},
	select: {id: true, displayName: true},
});

const locations = await prismaClient.location.findMany({
	where: {
		tenant: {
			in: tenantIds,
		},
	},
	select: {id: true, name: true},
});

const customers = await prismaClient.customers.findMany({
	where: {
		tenant: {
			in: tenantIds,
		},
	},
	select: {
		id: true,
		given: true,
		surname: true,
		email: true,
		locationId: true,
	},
});

const smses = await prismaClient.communications_SMS.findMany({
	where: {
		tenant: {
			in: tenantIds,
		},
		isInbound: false,
		sendPending: false,
	},
	select: {
		id: true,
		tenant: true,
		sentFrom: true,
		sentTo: true,
		customerId: true,
		locationId: true,
		content: true,
		billingUnits: true,
		timestamp: true,
	},
});

let fileHandle: FileHandle | undefined;
try {
	const fileHandle = await fs.open("/tmp/sms_emails_summary_3.csv", "w");

	await fileHandle.write(
		`"SMS ID","Tenant ID","Tenant Name","Sent From","Sent To","Customer ID","Customer Name","Customer Email","Location ID","Location Name","Content","Billing Units","Timestamp","Has Non GSM Characters","Has Backtick Character"\n`,
	);
	await fileHandle.sync();

	let count = 0;
	for (const sms of smses) {
		const tenantName = tenants.find(
			tenant => tenant.id === sms.tenant,
		)?.displayName;

		let locationId = sms.locationId;
		let locationName: string | undefined = undefined;

		const customer = customers.find(customer => customer.id === sms.customerId);

		if (!locationId && customer?.locationId) {
			locationId = customer?.locationId;
		}

		if (locationId) {
			locationName = locations.find(location => location.id === locationId)?.name;
		}

		await fileHandle.write(
			`"${escapeCsv(sms.id)}","${escapeCsv(sms.tenant)}","${tenantName ?? ""}","${escapeCsv(sms.sentFrom)}","${escapeCsv(sms.sentTo)}","${escapeCsv(customer?.id ?? "")}","${escapeCsv(customer ? customer.given + " " + customer.surname : "")}","${escapeCsv(customer?.email ?? "")}","${escapeCsv(locationId ?? "")}","${escapeCsv(locationName ?? "")}","${escapeCsv(sms.content)}","${sms.billingUnits}","${escapeCsv(sms.timestamp.toISOString())}","${hasNonGsmCharacters(sms.content) ? "TRUE" : "FALSE"}","${hasBackTickCharacter(sms.content) ? "TRUE" : "FALSE"}"\n`,
		);

		if (count % 100 == 0) {
			console.log(`Processing: ${((count / smses.length) * 100).toFixed(2)}%`);
			await fileHandle.sync();
		}

		++count;
	}
} finally {
	if (fileHandle) {
		await fileHandle.sync();
		await fileHandle.close();
	}
}

console.log(`Processing: 100%`);
/*
const claims1 = await prismaClient.medicare_BulkBillClaim.findMany({
	where: {
		tenant: "org_DmEqPdFOF2QyQiat",
		status: {
			in: ["Error", "BatchError"],
		},
	},
});

console.log("claims1:");
console.log(claims1);

const claims2 = await prismaClient.medicare_BulkBillClaimBatch.findMany({
	where: {
		tenant: "org_DmEqPdFOF2QyQiat",
		batchStatus: "Error",
	},
});

console.log("claims2:");
console.log(claims2);

const providers = await prismaClient.providers.findMany({
	where: {
		tenant: "org_DmEqPdFOF2QyQiat",
	},
});

console.log("providers:", providers);
*/

/*


const prismaClient = await getPrismaClient();

const subTenants = await prismaClient.tenants_SubTenants.findMany({
	select: {tenantId: true, parentTenantId: true},
});

const tenants = await prismaClient.tenants.findMany({
	select: {id: true, displayName: true},
});

const smses = await prismaClient.communications_SMS.findMany({
	where: {
		timestamp: {
			gte: "2025-09-01T00:00:00Z",
			lt: "2025-12-01T00:00:00Z",
		},
		//		tenant: "org_RBQMf8HftxQV5Ies",
		isInbound: false,
		sendPending: false,
	},
	select: {
		id: true,
		tenant: true,
		sentFrom: true,
		sentTo: true,
		customerId: true,
		content: true,
		billingUnits: true,
		timestamp: true,
	},
});

console.log(
	`"id","tenantId","tenantName","parentTenantId","parentTenantName","sentFrom","sentTo","customerId","content","billingUnits","hasBackTick","timestamp"`,
);
smses.forEach(sms => {
	if (hasNonGsmCharacters(sms.content)) {
		const tenant = tenants.find(tenant => tenant.id === sms.tenant);
		const subTenant = subTenants.find(
			subTenant => subTenant.tenantId === sms.tenant,
		);
		const parentTenant =
			subTenant && tenants.find(tenant => tenant.id === subTenant.parentTenantId);
		console.log(
			`"${escapeCsv(sms.id)}","${escapeCsv(tenant?.id ?? "")}","${escapeCsv(tenant?.displayName ?? "")}","${escapeCsv(parentTenant?.id ?? "")}","${escapeCsv(parentTenant?.displayName ?? "")}","${escapeCsv(sms.sentFrom)}","${escapeCsv(sms.sentTo)}","${escapeCsv(sms.customerId)}","${escapeCsv(sms.content)}","${sms.billingUnits}","${hasBackTickCharacter(sms.content)}","${sms.timestamp.toISOString()}"`,
		);
	}
});
*/

/*
const platformBillings = await prismaClient.platform_Billing.findMany({
	where: {
		timestamp: {gt: "2025-10-01T00:00:00Z", lt: "2025-10-30T23:59:59Z"},
	},
	select: {tenant: true, type: true, timestamp: true, units: true},
});

interface BillingSummary {
	tenantId: string;
	tenantName: string | undefined;
	parentTenantId: string | undefined;
	parentTenantName: string | undefined;
	sms: number[];
	email: number[];
}
const summaryBillings: Record<string, BillingSummary> = {};

platformBillings.forEach(platformBilling => {
	let summaryBilling = summaryBillings[platformBilling.tenant];
	if (!summaryBilling) {
		const tenant = tenants.find(tenant => tenant.id === platformBilling.tenant);
		const subTenant = subTenants.find(
			subTenant => subTenant.tenantId === platformBilling.tenant,
		);
		const parentTenant =
			subTenant && tenants.find(tenant => tenant.id === subTenant.parentTenantId);

		summaryBilling = {
			tenantId: platformBilling.tenant,
			tenantName: tenant?.displayName,
			parentTenantId: parentTenant?.id,
			parentTenantName: parentTenant?.displayName,
			sms: [
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0,
			],
			email: [
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0,
			],
		};
		summaryBillings[platformBilling.tenant] = summaryBilling;
	}
	const index = platformBilling.timestamp.getUTCDate() - 1;
	if (platformBilling.type.toUpperCase() === "SMS") {
		summaryBilling.sms[index] += platformBilling.units;
	}
	if (platformBilling.type.toUpperCase() === "EMAIL") {
		summaryBilling.email[index] += platformBilling.units;
	}
});

console.log(
	`"Tenant ID","Tenant Name","Parent Tenant ID","Parent Tenant Name","Date","Type","Value"`,
);
Object.values(summaryBillings).forEach(summaryBilling => {
	summaryBilling.sms.forEach((value, index) => {
		const date = `${index + 1}/10/2025`;
		console.log(
			`"${summaryBilling.tenantId}","${summaryBilling.tenantName}","${summaryBilling.parentTenantId ?? ""}","${summaryBilling.parentTenantName ?? ""}","${date}","SMS",${value}`,
		);
	});
});
Object.values(summaryBillings).forEach(summaryBilling => {
	summaryBilling.email.forEach((value, index) => {
		const date = `${index + 1}/10/2025`;
		console.log(
			`"${summaryBilling.tenantId}","${summaryBilling.tenantName}","${summaryBilling.parentTenantId ?? ""}","${summaryBilling.parentTenantName ?? ""}","${date}","EMAIL",${value}`,
		);
	});
});
*/
