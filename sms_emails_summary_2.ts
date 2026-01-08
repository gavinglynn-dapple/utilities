import {getPrismaClient} from "../src/prisma/Primsa";

const smsGsmCharacters =
	" @ΔSP0¡P¿p£_!1AQaq$Φ\"2BRbr¥Γ#3CScsèΛ¤4DTdtéΩ%5EUeuùΠ&6FVfvìΨ'7GWgwòΣ(8HXhxÇΘ)9IYiy\nΞ*:JZjzØ\f\x1B;KÄkäøÆ,<LÖlö\ræ-=MÑmñÅß.>NÜnüåÉ/?O§oà|^€{}[~]\\";

const escapeCsv = (value: string | undefined | null) =>
	value ? value.replace(/"/g, '""') : "";

const hasNonGsmCharacters = (value: string | undefined | null): boolean => {
	const localValue = value ?? "";
	for (let index = 0; index < localValue.length; ++index) {
		const ch = localValue[index];
		if (!smsGsmCharacters.includes(ch)) {
			/*			console.log(
				`Value: ${localValue}\nIndex: ${index}\nCharacter: ${ch} (0x${ch.charCodeAt(0).toString(16)})`,
			);*/
			return true;
		}
	}
	return false;
};

const hasBackTickCharacter = (value: string | undefined): boolean => {
	const localValue = value ?? "";
	return localValue.includes("\u2019");
};

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
